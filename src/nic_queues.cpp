// Copyright (C) 2026 Sunny Bains
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

#include "sysbench/nic_queues.hpp"

#include "sysbench/memory.hpp"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <linux/genetlink.h>
#include <linux/netdev.h>
#include <linux/netlink.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <new>

namespace sb::nic {
namespace {

constexpr unsigned kMaxCpus = 1024;
constexpr unsigned kMaxQueues = 1024;
constexpr unsigned kMaxInterfaces = 8;

struct Table {
    int m_ifindex = 0;
    std::uint32_t m_channels = 0;
    // NAPI id each CPU transmits on (0: unknown).
    std::uint32_t m_napi_of_cpu[kMaxCpus]{};
};

// Built once per interface at connection set-up and published with a CAS:
// no lock, and the hot path never comes here.
std::array<std::atomic<Table*>, kMaxInterfaces> g_tables{};

int interface_of_socket(int fd) noexcept {
    sockaddr_storage local{};
    socklen_t length = sizeof(local);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &length) != 0) return 0;
    ifaddrs* list = nullptr;
    if (::getifaddrs(&list) != 0) return 0;
    int ifindex = 0;
    for (ifaddrs* entry = list; entry != nullptr && ifindex == 0; entry = entry->ifa_next) {
        if (entry->ifa_addr == nullptr || entry->ifa_addr->sa_family != local.ss_family) continue;
        bool same = false;
        if (local.ss_family == AF_INET) {
            same = reinterpret_cast<const sockaddr_in*>(entry->ifa_addr)->sin_addr.s_addr ==
                   reinterpret_cast<const sockaddr_in*>(&local)->sin_addr.s_addr;
        } else if (local.ss_family == AF_INET6) {
            same = std::memcmp(&reinterpret_cast<const sockaddr_in6*>(entry->ifa_addr)->sin6_addr,
                               &reinterpret_cast<const sockaddr_in6*>(&local)->sin6_addr,
                               sizeof(in6_addr)) == 0;
        }
        if (same) ifindex = static_cast<int>(::if_nametoindex(entry->ifa_name));
    }
    ::freeifaddrs(list);
    return ifindex;
}

// ---- netdev generic netlink --------------------------------------------------

struct Attr {
    const unsigned char* m_data = nullptr;
    std::uint16_t m_length = 0;
};

void parse_attrs(const unsigned char* data, std::size_t length, Attr* out, unsigned max) {
    std::size_t offset = 0;
    while (offset + NLA_HDRLEN <= length) {
        const auto* attr = reinterpret_cast<const nlattr*>(data + offset);
        if (attr->nla_len < NLA_HDRLEN || offset + attr->nla_len > length) break;
        const unsigned type = attr->nla_type & NLA_TYPE_MASK;
        if (type < max) out[type] = Attr{data + offset + NLA_HDRLEN,
                                         static_cast<std::uint16_t>(attr->nla_len - NLA_HDRLEN)};
        offset += NLA_ALIGN(attr->nla_len);
    }
}

std::uint32_t attr_u32(const Attr& attr) {
    std::uint32_t value = 0;
    if (attr.m_length >= 4) std::memcpy(&value, attr.m_data, 4);
    return value;
}

std::size_t put_attr(unsigned char* at, std::uint16_t type, const void* data, std::uint16_t length) {
    auto* attr = reinterpret_cast<nlattr*>(at);
    attr->nla_type = type;
    attr->nla_len = static_cast<std::uint16_t>(NLA_HDRLEN + length);
    std::memcpy(at + NLA_HDRLEN, data, length);
    return NLA_ALIGN(attr->nla_len);
}

class Netlink {
public:
    Netlink() : m_fd(::socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_GENERIC)) {}
    ~Netlink() {
        if (m_fd >= 0) ::close(m_fd);
    }
    [[nodiscard]] bool valid() const noexcept { return m_fd >= 0; }

    // Send one generic netlink request and call `on_message(attrs, len)` for
    // each reply.  False on error.
    template <class OnMessage>
    bool request(std::uint16_t family, std::uint8_t command, std::uint16_t flags,
                 const unsigned char* payload, std::size_t payload_length, OnMessage&& on_message) {
        unsigned char message[256]{};
        auto* header = reinterpret_cast<nlmsghdr*>(message);
        auto* generic = reinterpret_cast<genlmsghdr*>(message + NLMSG_HDRLEN);
        header->nlmsg_len = static_cast<std::uint32_t>(NLMSG_HDRLEN + GENL_HDRLEN + payload_length);
        header->nlmsg_type = family;
        header->nlmsg_flags = static_cast<std::uint16_t>(NLM_F_REQUEST | flags);
        header->nlmsg_seq = ++m_sequence;
        generic->cmd = command;
        generic->version = 1;
        std::memcpy(message + NLMSG_HDRLEN + GENL_HDRLEN, payload, payload_length);
        if (::send(m_fd, message, header->nlmsg_len, 0) < 0) return false;
        for (;;) {
            const ssize_t received = ::recv(m_fd, m_buffer, sizeof(m_buffer), 0);
            if (received <= 0) return false;
            std::size_t offset = 0;
            while (offset + NLMSG_HDRLEN <= static_cast<std::size_t>(received)) {
                const auto* reply = reinterpret_cast<const nlmsghdr*>(m_buffer + offset);
                if (reply->nlmsg_len < NLMSG_HDRLEN ||
                    offset + reply->nlmsg_len > static_cast<std::size_t>(received)) {
                    return false;
                }
                if (reply->nlmsg_type == NLMSG_DONE) return true;
                if (reply->nlmsg_type == NLMSG_ERROR) {
                    const auto* error = reinterpret_cast<const nlmsgerr*>(m_buffer + offset + NLMSG_HDRLEN);
                    return error->error == 0;
                }
                const unsigned char* body = m_buffer + offset + NLMSG_HDRLEN + GENL_HDRLEN;
                const std::size_t body_length = reply->nlmsg_len - NLMSG_HDRLEN - GENL_HDRLEN;
                on_message(body, body_length);
                offset += NLMSG_ALIGN(reply->nlmsg_len);
            }
            if ((flags & NLM_F_DUMP) == 0) return true;
        }
    }

private:
    int m_fd = -1;
    std::uint32_t m_sequence = 0;
    alignas(8) unsigned char m_buffer[64 * 1024]{};
};

std::uint16_t netdev_family(Netlink& link) {
    unsigned char payload[64];
    const std::size_t length = put_attr(payload, CTRL_ATTR_FAMILY_NAME, NETDEV_FAMILY_NAME,
                                        sizeof(NETDEV_FAMILY_NAME));
    std::uint16_t family = 0;
    (void)link.request(GENL_ID_CTRL, CTRL_CMD_GETFAMILY, 0, payload, length,
                       [&](const unsigned char* body, std::size_t body_length) {
                           Attr attrs[CTRL_ATTR_MAX + 1]{};
                           parse_attrs(body, body_length, attrs, CTRL_ATTR_MAX + 1);
                           if (attrs[CTRL_ATTR_FAMILY_ID].m_length >= 2) {
                               std::memcpy(&family, attrs[CTRL_ATTR_FAMILY_ID].m_data, 2);
                           }
                       });
    return family;
}

// Parse a sysfs CPU bitmap ("000000,00004000") into `cpus`.
template <class OnCpu>
void for_each_cpu_in_mask(const char* text, OnCpu&& on_cpu) {
    unsigned words[64];
    unsigned count = 0;
    const char* at = text;
    while (*at != '\0' && *at != '\n' && count < 64) {
        unsigned word = 0;
        while (*at != '\0' && *at != ',' && *at != '\n') {
            const char c = *at++;
            word <<= 4;
            if (c >= '0' && c <= '9') word |= static_cast<unsigned>(c - '0');
            else if (c >= 'a' && c <= 'f') word |= static_cast<unsigned>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') word |= static_cast<unsigned>(c - 'A' + 10);
        }
        words[count++] = word;
        if (*at == ',') ++at;
    }
    for (unsigned index = 0; index < count; ++index) {
        const unsigned word = words[count - 1 - index];  // last word is CPUs 0-31
        for (unsigned bit = 0; bit < 32; ++bit) {
            if ((word >> bit) & 1u) on_cpu(index * 32 + bit);
        }
    }
}

bool read_file(const char* path, char* out, std::size_t size) {
    FILE* file = std::fopen(path, "r");
    if (file == nullptr) return false;
    const std::size_t got = std::fread(out, 1, size - 1, file);
    std::fclose(file);
    out[got] = '\0';
    return got != 0;
}

Table* build_table(int ifindex) {
    void* memory = mem::global_root().allocate(sizeof(Table), alignof(Table));
    auto* table = ::new (memory) Table{};
    table->m_ifindex = ifindex;

    Netlink link;
    if (!link.valid()) return table;
    const std::uint16_t family = netdev_family(link);
    if (family == 0) return table;

    // Queue -> NAPI id, for TX and RX queues.
    static thread_local std::uint32_t tx_napi[kMaxQueues];
    static thread_local std::uint32_t napi_irq_napi[kMaxQueues];
    static thread_local std::uint32_t napi_irq[kMaxQueues];
    std::memset(tx_napi, 0, sizeof(tx_napi));
    unsigned napis = 0;
    unsigned char payload[16];
    const auto index = static_cast<std::uint32_t>(ifindex);
    std::size_t length = put_attr(payload, NETDEV_A_QUEUE_IFINDEX, &index, 4);
    (void)link.request(family, NETDEV_CMD_QUEUE_GET, NLM_F_DUMP, payload, length,
                       [&](const unsigned char* body, std::size_t body_length) {
                           Attr attrs[NETDEV_A_QUEUE_MAX + 1]{};
                           parse_attrs(body, body_length, attrs, NETDEV_A_QUEUE_MAX + 1);
                           const std::uint32_t id = attr_u32(attrs[NETDEV_A_QUEUE_ID]);
                           const std::uint32_t type = attr_u32(attrs[NETDEV_A_QUEUE_TYPE]);
                           const std::uint32_t napi = attr_u32(attrs[NETDEV_A_QUEUE_NAPI_ID]);
                           if (type == NETDEV_QUEUE_TYPE_RX) ++table->m_channels;
                           if (type == NETDEV_QUEUE_TYPE_TX && id < kMaxQueues) tx_napi[id] = napi;
                       });
    length = put_attr(payload, NETDEV_A_NAPI_IFINDEX, &index, 4);
    (void)link.request(family, NETDEV_CMD_NAPI_GET, NLM_F_DUMP, payload, length,
                       [&](const unsigned char* body, std::size_t body_length) {
                           Attr attrs[NETDEV_A_NAPI_MAX + 1]{};
                           parse_attrs(body, body_length, attrs, NETDEV_A_NAPI_MAX + 1);
                           if (napis < kMaxQueues) {
                               napi_irq_napi[napis] = attr_u32(attrs[NETDEV_A_NAPI_ID]);
                               napi_irq[napis] = attr_u32(attrs[NETDEV_A_NAPI_IRQ]);
                               ++napis;
                           }
                       });

    char name[IF_NAMESIZE]{};
    if (::if_indextoname(static_cast<unsigned>(ifindex), name) == nullptr) return table;

    // XPS: the TX queue each CPU sends on.
    char path[256];
    char text[512];
    for (unsigned queue = 0; queue < kMaxQueues; ++queue) {
        std::snprintf(path, sizeof(path), "/sys/class/net/%s/queues/tx-%u/xps_cpus", name, queue);
        if (!read_file(path, text, sizeof(text))) {
            if (queue > 0) {
                std::snprintf(path, sizeof(path), "/sys/class/net/%s/queues/tx-%u", name, queue);
                if (::access(path, F_OK) != 0) break;
            }
            continue;
        }
        if (tx_napi[queue] == 0) continue;
        for_each_cpu_in_mask(text, [&](unsigned cpu) {
            if (cpu < kMaxCpus && table->m_napi_of_cpu[cpu] == 0) {
                table->m_napi_of_cpu[cpu] = tx_napi[queue];
            }
        });
    }
    // No XPS for a CPU: the channel whose interrupt is affine to it.
    for (unsigned n = 0; n < napis; ++n) {
        if (napi_irq[n] == 0) continue;
        std::snprintf(path, sizeof(path), "/proc/irq/%u/smp_affinity_list", napi_irq[n]);
        if (!read_file(path, text, sizeof(text))) continue;
        unsigned cpu = 0;
        if (std::sscanf(text, "%u", &cpu) == 1 && cpu < kMaxCpus &&
            table->m_napi_of_cpu[cpu] == 0) {
            table->m_napi_of_cpu[cpu] = napi_irq_napi[n];
        }
    }
    return table;
}

const Table* table_for(int fd) noexcept {
    const int ifindex = interface_of_socket(fd);
    if (ifindex == 0) return nullptr;
    for (auto& slot : g_tables) {
        Table* table = slot.load(std::memory_order_acquire);
        if (table != nullptr && table->m_ifindex == ifindex) return table;
    }
    Table* built = nullptr;
    try {
        built = build_table(ifindex);
    } catch (...) {
        return nullptr;
    }
    for (auto& slot : g_tables) {
        Table* expected = nullptr;
        if (slot.compare_exchange_strong(expected, built, std::memory_order_acq_rel)) return built;
        if (expected->m_ifindex == ifindex) return expected;  // built concurrently
    }
    return built;
}

}  // namespace

std::uint32_t napi_for_cpu(int fd, int cpu) noexcept {
    if (cpu < 0 || static_cast<unsigned>(cpu) >= kMaxCpus) return 0;
    const Table* table = table_for(fd);
    return table == nullptr ? 0 : table->m_napi_of_cpu[cpu];
}

std::uint32_t incoming_napi(int fd) noexcept {
#ifdef SO_INCOMING_NAPI_ID
    std::uint32_t napi = 0;
    socklen_t length = sizeof(napi);
    if (::getsockopt(fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi, &length) != 0) return 0;
    return napi;
#else
    (void)fd;
    return 0;
#endif
}

std::uint32_t channel_count(int fd) noexcept {
    const Table* table = table_for(fd);
    return table == nullptr ? 0 : table->m_channels;
}

}  // namespace sb::nic
