#pragma once

// Minimal, allocation-conscious coroutine task used by the runtime.  Tasks are
// lazy; the runtime starts them.  Awaiting a task schedules it on the same
// worker, so sockets, Lua states and arenas never migrate between threads.

#include "sysbench/memory.hpp"

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace sb {

namespace detail {

struct PromiseBase;

using CompleteFn = void (*)(PromiseBase*) noexcept;

struct PromiseBase {
    std::coroutine_handle<> self{};
    PromiseBase* continuation = nullptr;
    PromiseBase* next_ready = nullptr;
    PromiseBase* next_completed = nullptr;
    PromiseBase* next_timer = nullptr;
    PromiseBase* next_deferred = nullptr;
    CompleteFn on_complete = nullptr;
    std::exception_ptr error;
    std::chrono::steady_clock::time_point wake_at{};
    std::uint32_t owner_worker = 0;
    bool in_ready = false;
    bool timer_queued = false;
    bool awaited = false;
    bool started = false;
};

extern thread_local PromiseBase* tls_current;
extern thread_local void* tls_worker;

void schedule_current(PromiseBase* task) noexcept;
void sleep_current(std::chrono::nanoseconds duration) noexcept;
void defer_destroy(PromiseBase* task) noexcept;
[[noreturn]] void no_runtime();

struct FinalAwaiter {
    [[nodiscard]] bool await_ready() const noexcept { return false; }

    template <class Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> handle) noexcept {
        PromiseBase& promise = handle.promise();
        if (promise.continuation != nullptr) {
            PromiseBase* continuation = promise.continuation;
            promise.continuation = nullptr;
            tls_current = continuation;
            return continuation->self;
        }
        if (promise.on_complete != nullptr) {
            promise.on_complete(&promise);
        }
        return std::noop_coroutine();
    }

    void await_resume() const noexcept {}
};

template <class T>
class TaskAwaiter {
public:
    using Handle = std::coroutine_handle<T>;

    explicit TaskAwaiter(Handle handle) noexcept : m_handle(handle) {}
    TaskAwaiter(const TaskAwaiter&) = delete;
    TaskAwaiter& operator=(const TaskAwaiter&) = delete;
    TaskAwaiter(TaskAwaiter&& other) noexcept
        : m_handle(std::exchange(other.m_handle, {})) {}
    ~TaskAwaiter() {
        if (m_handle) m_handle.destroy();
    }

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) noexcept {
        PromiseBase* parent = tls_current;
        if (parent == nullptr) no_runtime();
        PromiseBase& child = m_handle.promise();
        child.continuation = parent;
        child.started = true;
        tls_current = &child;
        // Symmetric transfer: start the child in this resume instead of
        // queueing it for another scheduler iteration.  A MySQL query nests
        // several task levels (query -> command -> packet -> exact read), so
        // routing every level through the ready queue costs several
        // microsecond-scale scheduler turns per query.
        return m_handle;
    }

    decltype(auto) await_resume() {
        auto& promise = m_handle.promise();
        if (promise.error) {
            std::exception_ptr error = promise.error;
            detail::defer_destroy(&promise);
            m_handle = {};
            std::rethrow_exception(error);
        }
        using Value = typename T::Value;
        if constexpr (!std::is_void_v<Value>) {
            Value value = std::move(*promise.value);
            detail::defer_destroy(&promise);
            m_handle = {};
            return value;
        } else {
            detail::defer_destroy(&promise);
            m_handle = {};
        }
    }

private:
    Handle m_handle;
};

template <class T>
class Task {
public:
    struct promise_type final : PromiseBase {
        using Value = T;
        std::optional<T> value;

        Task get_return_object() noexcept {
            auto handle = std::coroutine_handle<promise_type>::from_promise(*this);
            this->self = handle;
            return Task(handle);
        }

        std::suspend_always initial_suspend() const noexcept { return {}; }
        FinalAwaiter final_suspend() const noexcept { return {}; }

        void return_value(T result) {
            value.emplace(std::move(result));
        }

        void unhandled_exception() noexcept {
            this->error = std::current_exception();
        }

        static void* operator new(std::size_t size) {
            return mem::allocate_task_frame(size);
        }
        static void operator delete(void* pointer, std::size_t size) noexcept {
            mem::deallocate_task_frame(pointer, size);
        }
        static void operator delete(void* pointer) noexcept {
            mem::deallocate_task_frame(pointer, 0);
        }
    };

    using Handle = std::coroutine_handle<promise_type>;

    Task() noexcept = default;
    explicit Task(Handle handle) noexcept : m_handle(handle) {}
    Task(Task&& other) noexcept : m_handle(std::exchange(other.m_handle, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (m_handle && !m_handle.promise().started) m_handle.destroy();
            m_handle = std::exchange(other.m_handle, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (m_handle && !m_handle.promise().started) m_handle.destroy();
    }

    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(m_handle); }
    [[nodiscard]] Handle release() noexcept { return std::exchange(m_handle, {}); }
    [[nodiscard]] TaskAwaiter<promise_type> operator co_await() && noexcept {
        m_handle.promise().awaited = true;
        return TaskAwaiter<promise_type>(release());
    }

private:
    Handle m_handle{};
};

template <>
class Task<void> {
public:
    struct promise_type final : PromiseBase {
        using Value = void;
        Task get_return_object() noexcept {
            auto handle = std::coroutine_handle<promise_type>::from_promise(*this);
            this->self = handle;
            return Task(handle);
        }

        std::suspend_always initial_suspend() const noexcept { return {}; }
        FinalAwaiter final_suspend() const noexcept { return {}; }
        void return_void() const noexcept {}
        void unhandled_exception() noexcept {
            this->error = std::current_exception();
        }

        static void* operator new(std::size_t size) {
            return mem::allocate_task_frame(size);
        }
        static void operator delete(void* pointer, std::size_t size) noexcept {
            mem::deallocate_task_frame(pointer, size);
        }
        static void operator delete(void* pointer) noexcept {
            mem::deallocate_task_frame(pointer, 0);
        }
    };

    using Handle = std::coroutine_handle<promise_type>;

    Task() noexcept = default;
    explicit Task(Handle handle) noexcept : m_handle(handle) {}
    Task(Task&& other) noexcept : m_handle(std::exchange(other.m_handle, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (m_handle && !m_handle.promise().started) m_handle.destroy();
            m_handle = std::exchange(other.m_handle, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (m_handle && !m_handle.promise().started) m_handle.destroy();
    }

    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(m_handle); }
    [[nodiscard]] Handle release() noexcept { return std::exchange(m_handle, {}); }
    [[nodiscard]] TaskAwaiter<promise_type> operator co_await() && noexcept {
        m_handle.promise().awaited = true;
        return TaskAwaiter<promise_type>(release());
    }

private:
    Handle m_handle{};
};

struct YieldAwaiter {
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) const noexcept {
        if (tls_current == nullptr) no_runtime();
        schedule_current(tls_current);
    }
    void await_resume() const noexcept {}
};

struct SleepAwaiter {
    std::chrono::nanoseconds duration;

    [[nodiscard]] bool await_ready() const noexcept {
        return duration <= std::chrono::nanoseconds::zero();
    }
    void await_suspend(std::coroutine_handle<>) const noexcept {
        sleep_current(duration);
    }
    void await_resume() const noexcept {}
};

[[nodiscard]] inline YieldAwaiter yield_now() noexcept { return {}; }

template <class Rep, class Period>
[[nodiscard]] SleepAwaiter sleep_for(
    std::chrono::duration<Rep, Period> duration) noexcept {
    return SleepAwaiter{std::chrono::duration_cast<std::chrono::nanoseconds>(duration)};
}

}  // namespace detail

using detail::Task;
using detail::sleep_for;
using detail::yield_now;

}  // namespace sb
