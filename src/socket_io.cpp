#include "sysbench/socket_io.hpp"

#include <pthread.h>
#include <sched.h>

#include <cerrno>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "sysbench/numa.hpp"

namespace sb {
namespace {

constexpr std::size_t kSubmitBatch = 64;

void pin_to_node(int node) noexcept {
    if (node < 0) return;
    (void)numa::set_preferred(node);
    const numa::Topology& topology = numa::Topology::instance();
    const numa::NodeInfo* info = topology.node_by_id(node);
    if (info == nullptr) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    for (std::uint32_t i = 0; i < info->cpu_count; ++i) {
        const int cpu = static_cast<int>(info->cpus[i]);
        if (cpu != topology.reserved_cpu()) CPU_SET(cpu, &set);
    }
    (void)::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
}

bool enqueue_awaiter(SocketIo& io, IoRequest& request) noexcept {
    request.completion.task = detail::tls_current;
    if (request.completion.task == nullptr) detail::no_runtime();
    if (!io.enqueue(request)) {
        request.completion.result = -ENOBUFS;
        detail::schedule_current(request.completion.task);
        return false;
    }
    return true;
}

}  // namespace

SocketIo::SocketIo(Runtime& runtime, std::uint32_t worker_id, unsigned entries,
                   unsigned buffer_slots, unsigned buffer_size)
    : m_numa_node(runtime.worker_numa_node(worker_id)),
      m_ring(entries),
      m_buffers(m_ring, mem::global_root(), buffer_size, buffer_slots,
               m_numa_node),
      m_runtime(&runtime),
      m_worker_id(worker_id) {
    m_cq_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (m_cq_fd >= 0) {
        m_cq_eventfd_registered =
            io_uring_register_eventfd(&m_ring.native_ring(), m_cq_fd) == 0;
        if (!m_cq_eventfd_registered) {
            ::close(m_cq_fd);
            m_cq_fd = -1;
        }
    }
    m_shutdown_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (m_shutdown_fd < 0) {
        if (m_cq_eventfd_registered) {
            (void)io_uring_unregister_eventfd(&m_ring.native_ring());
            m_cq_eventfd_registered = false;
        }
        if (m_cq_fd >= 0) {
            ::close(m_cq_fd);
            m_cq_fd = -1;
        }
        throw std::system_error(errno, std::generic_category(), "eventfd");
    }
    if (!runtime.start_reaper(*this)) {
        if (m_cq_eventfd_registered) {
            (void)io_uring_unregister_eventfd(&m_ring.native_ring());
            m_cq_eventfd_registered = false;
        }
        if (m_cq_fd >= 0) ::close(m_cq_fd);
        if (m_shutdown_fd >= 0) ::close(m_shutdown_fd);
        throw std::runtime_error("runtime reaper registry is full");
    }
}

SocketIo::~SocketIo() {
    request_stop();
    m_runtime->stop_reaper(*this);
    if (m_cq_eventfd_registered) {
        (void)io_uring_unregister_eventfd(&m_ring.native_ring());
        m_cq_eventfd_registered = false;
    }
    if (m_cq_fd >= 0) {
        ::close(m_cq_fd);
        m_cq_fd = -1;
    }
    if (m_shutdown_fd >= 0) {
        ::close(m_shutdown_fd);
        m_shutdown_fd = -1;
    }
}

bool SocketIo::enqueue(IoRequest& request) noexcept {
    if (!m_running.load(std::memory_order_acquire) ||
        Runtime::current_worker_id() != m_worker_id) {
        return false;
    }
    // The owning worker is the only producer and it drains the ring into the
    // SQ on every enqueue, so a full ring means the submission path failed.
    // Never wait for space here; all waiting policy belongs to the Runtime.
    if (!m_submit_ring.try_push(&request)) return false;
    Runtime::note_io_submitted();
    submit_pending();
    return true;
}

void SocketIo::submit_pending() noexcept {
    IoRequest* batch[kSubmitBatch];
    std::size_t count = 0;
    IoRequest* request = nullptr;
    while (count < kSubmitBatch && m_submit_ring.try_pop(request)) {
        batch[count++] = request;
    }
    if (count == 0) return;

    bool submitted = false;
    for (std::size_t i = 0; i < count; ++i) {
        IoRequest* item = batch[i];
        bool ok = false;
        switch (item->operation) {
            case IoOperation::ReadFixed:
                ok = m_ring.submit_read_fixed(item->fd, item->buffer, item->length,
                                             item->slot, &item->completion);
                break;
            case IoOperation::WriteFixed:
                ok = m_ring.submit_write_fixed(item->fd, item->const_buffer,
                                              item->length, item->slot,
                                              &item->completion);
                break;
            case IoOperation::Connect:
                ok = m_ring.submit_connect(item->fd, item->address,
                                          item->address_length, &item->completion);
                break;
            case IoOperation::Accept:
                ok = m_ring.submit_accept(item->fd, const_cast<sockaddr*>(item->address),
                                         item->result_length, &item->completion);
                break;
            case IoOperation::Recv:
                ok = m_ring.submit_recv(item->fd, item->buffer, item->length,
                                       item->flags, &item->completion);
                break;
            case IoOperation::Send:
                ok = m_ring.submit_send(item->fd, item->const_buffer, item->length,
                                       item->flags, &item->completion);
                break;
        }
        if (!ok) {
            m_runtime->post_completion(m_worker_id, item->completion.task);
            m_runtime->note_io_completed(m_worker_id);
        } else {
            submitted = true;
        }
    }
    if (submitted) (void)m_ring.flush_submit();
}

bool SocketIo::reap() noexcept {
    bool reaped = false;
    while (io_uring_cqe* cqe = m_ring.peek_completion()) {
        auto* completion =
            static_cast<IoCompletion*>(io_uring_cqe_get_data(cqe));
        const int cqe_result = cqe->res;
        m_ring.mark_seen(cqe);
        if (completion != nullptr) completion->result = cqe_result;
        if (completion != nullptr && completion->task != nullptr) {
            m_runtime->post_completion(m_worker_id, completion->task);
        }
        // Publish the completion before decrementing pending_io: a worker that
        // observes zero pending operations then cannot miss the posted task,
        // so it does not park and pay an eventfd wakeup for a completion that
        // has already arrived.
        m_runtime->note_io_completed(m_worker_id);
        reaped = true;
    }
    return reaped;
}

void SocketIo::pin() noexcept {
    pin_to_node(m_numa_node);
}

void SocketIo::request_stop() noexcept {
    m_running.store(false, std::memory_order_release);
    if (m_shutdown_fd >= 0) {
        const std::uint64_t one = 1;
        const ssize_t ignored = ::write(m_shutdown_fd, &one, sizeof(one));
        (void)ignored;
    }
}

bool SocketIo::wait() noexcept {
    // Block until the kernel signals a CQ eventfd (one per posted completion)
    // or until shutdown.  No spinning: the Runtime owns that policy.
    if (!m_cq_eventfd_registered) {
        __kernel_timespec timeout{0, 10'000'000};
        const int result = m_ring.wait_completion(&timeout);
        return !(result < 0 && result != -ETIME && result != -EINTR);
    }

    pollfd descriptors[2] = {
        pollfd{.fd = m_cq_fd, .events = POLLIN, .revents = 0},
        pollfd{.fd = m_shutdown_fd, .events = POLLIN, .revents = 0},
    };
    const int result = ::poll(descriptors, 2, -1);
    if (result < 0) return errno == EINTR;
    if ((descriptors[0].revents & POLLIN) != 0) {
        std::uint64_t counter = 0;
        const ssize_t ignored = ::read(m_cq_fd, &counter, sizeof(counter));
        (void)ignored;
    }
    return (descriptors[1].revents & POLLIN) == 0;
}


void SocketReadAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketWriteAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketConnectAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketAcceptAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketRecvAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketSendAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}

}  // namespace sb
