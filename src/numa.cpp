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

#include "sysbench/numa.hpp"

#include <numa.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <linux/mempolicy.h>

#include <algorithm>
#include <cstdio>
#include <cstring>

namespace sb::numa {
namespace {

constexpr std::uint32_t kPageSize = 4096;

int read_text_int(const char* path) {
    const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    char buffer[32]{};
    const ssize_t length = ::read(fd, buffer, sizeof(buffer) - 1);
    ::close(fd);
    if (length <= 0) return -1;
    int value = 0;
    for (ssize_t i = 0; i < length; ++i) {
        if (buffer[i] < '0' || buffer[i] > '9') break;
        value = value * 10 + (buffer[i] - '0');
    }
    return value;
}

std::uint64_t core_key(std::uint32_t cpu) {
    char path[128]{};
    std::snprintf(path, sizeof(path),
                  "/sys/devices/system/cpu/cpu%u/topology/physical_package_id", cpu);
    int package = read_text_int(path);
    std::snprintf(path, sizeof(path),
                  "/sys/devices/system/cpu/cpu%u/topology/core_id", cpu);
    int core = read_text_int(path);
    if (package < 0) package = 0;
    if (core < 0) return cpu;  // unique fallback
    return (static_cast<std::uint64_t>(package) << 32) |
           static_cast<std::uint32_t>(core);
}

void order_cpus_by_core(const std::uint32_t* raw, std::uint32_t count,
                        std::uint32_t* ordered) {
    std::uint64_t keys[kMaxNodeCpus]{};
    bool emitted[kMaxNodeCpus]{};
    for (std::uint32_t i = 0; i < count; ++i) keys[i] = core_key(raw[i]);

    std::uint32_t output = 0;
    // First thread of each physical core.
    for (std::uint32_t i = 0; i < count; ++i) {
        bool first = true;
        for (std::uint32_t j = 0; j < i; ++j) {
            if (keys[j] == keys[i]) {
                first = false;
                break;
            }
        }
        if (first) {
            ordered[output++] = raw[i];
            emitted[i] = true;
        }
    }
    // SMT siblings.
    for (std::uint32_t i = 0; i < count; ++i) {
        if (!emitted[i]) ordered[output++] = raw[i];
    }
}

}  // namespace

const Topology& Topology::instance() {
    static Topology topology;
    return topology;
}

Topology::Topology() {
    m_cpu_to_node.fill(-1);
    const int current_cpu = ::sched_getcpu();
    m_reserved_cpu = current_cpu >= 0 ? current_cpu : -1;
    m_available = numa_available() >= 0;

    if (m_available) {
        const int maximum = numa_max_node();
        for (int id = 0; id <= maximum && m_node_count < kMaxNodes; ++id) {
            if (numa_all_nodes_ptr == nullptr ||
                !numa_bitmask_isbitset(numa_all_nodes_ptr, static_cast<unsigned>(id))) {
                continue;
            }
            bitmask* cpus = numa_allocate_cpumask();
            if (cpus == nullptr) continue;
            if (numa_node_to_cpus(id, cpus) != 0) {
                numa_bitmask_free(cpus);
                continue;
            }
            std::uint32_t raw[kMaxNodeCpus]{};
            std::uint32_t count = 0;
            const unsigned limit =
                std::min<unsigned>(static_cast<unsigned>(numa_num_configured_cpus()), kMaxCpuCount);
            for (unsigned cpu = 0; cpu < limit && count < kMaxNodeCpus; ++cpu) {
                if (numa_bitmask_isbitset(cpus, cpu) &&
                    (numa_all_cpus_ptr == nullptr ||
                     numa_bitmask_isbitset(numa_all_cpus_ptr, cpu))) {
                    m_cpu_to_node[cpu] = id;
                    raw[count++] = cpu;
                }
            }
            numa_bitmask_free(cpus);
            if (count == 0) continue;

            NodeInfo& node = m_nodes[m_node_count++];
            node.id = id;
            node.cpu_count = count;
            order_cpus_by_core(raw, count, node.cpus.data());
            for (std::uint32_t cpu = 0; cpu < count; ++cpu) {
                if (node.cpus[cpu] < kMaxCpuCount) {
                    m_cpu_to_node[node.cpus[cpu]] = id;
                }
            }
        }
    }

    if (m_node_count == 0) {
        // Fallback: one synthetic node covering the process affinity mask.
        NodeInfo& node = m_nodes[m_node_count++];
        node.id = 0;
        cpu_set_t set;
        CPU_ZERO(&set);
        if (::sched_getaffinity(0, sizeof(set), &set) == 0) {
            for (unsigned cpu = 0; cpu < kMaxCpuCount && cpu < CPU_SETSIZE; ++cpu) {
                if (CPU_ISSET(cpu, &set) && node.cpu_count < kMaxNodeCpus) {
                    m_cpu_to_node[cpu] = 0;
                    node.cpus[node.cpu_count++] = cpu;
                }
            }
        }
        if (node.cpu_count == 0) {
            const long online = ::sysconf(_SC_NPROCESSORS_ONLN);
            const std::uint32_t count =
                static_cast<std::uint32_t>(std::clamp<long>(online, 1, kMaxNodeCpus));
            for (std::uint32_t cpu = 0; cpu < count; ++cpu) {
                node.cpus[node.cpu_count++] = cpu;
                m_cpu_to_node[cpu] = 0;
            }
        }
    }

    const int current = ::sched_getcpu();
    m_home_node = current >= 0 ? node_of_cpu(static_cast<unsigned>(current)) : -1;
    if (m_home_node < 0) m_home_node = m_nodes[0].id;
    m_reserved_cpu = -1;
    for (std::uint32_t index = 0; index < m_node_count; ++index) {
        if (m_nodes[index].id == m_home_node && m_nodes[index].cpu_count > 0) {
            m_reserved_cpu = static_cast<int>(
                m_nodes[index].cpus[m_nodes[index].cpu_count - 1]);
            break;
        }
    }
}

const NodeInfo& Topology::node(std::uint32_t index) const noexcept {
    if (index >= m_node_count) return m_nodes[0];
    return m_nodes[index];
}

const NodeInfo* Topology::node_by_id(int id) const noexcept {
    for (std::uint32_t index = 0; index < m_node_count; ++index) {
        if (m_nodes[index].id == id) return &m_nodes[index];
    }
    return nullptr;
}

int Topology::node_of_cpu(unsigned cpu) const noexcept {
    if (cpu >= kMaxCpuCount) return -1;
    return m_cpu_to_node[cpu];
}

int Topology::node_for_worker(std::uint32_t worker) const noexcept {
    if (m_node_count == 0) return -1;
    return m_nodes[worker % m_node_count].id;
}

int Topology::cpu_for_worker(std::uint32_t worker) const noexcept {
    if (m_node_count == 0) return -1;
    const std::uint32_t node_index = worker % m_node_count;
    const NodeInfo& info = m_nodes[node_index];
    if (info.cpu_count == 0) return -1;
    const std::uint32_t ordinal = worker / m_node_count;
    std::uint32_t slot = ordinal % info.cpu_count;
    for (std::uint32_t attempts = 0; attempts < info.cpu_count; ++attempts) {
        const int cpu = static_cast<int>(info.cpus[slot]);
        if (cpu != m_reserved_cpu) return cpu;
        slot = (slot + 1) % info.cpu_count;
    }
    return static_cast<int>(info.cpus[0]);
}

bool bind_range(void* address, std::size_t length, int node) noexcept {
    if (address == nullptr || length == 0 || node < 0 || numa_available() < 0) {
        return false;
    }
    auto* mask = numa_allocate_nodemask();
    if (mask == nullptr) return false;
    numa_bitmask_setbit(mask, static_cast<unsigned>(node));

    const std::uintptr_t begin = reinterpret_cast<std::uintptr_t>(address);
    const std::uintptr_t aligned_begin = begin & ~(std::uintptr_t(kPageSize) - 1);
    const std::uintptr_t end = begin + length;
    const std::uintptr_t aligned_end =
        (end + kPageSize - 1) & ~(std::uintptr_t(kPageSize) - 1);
    const long result = ::syscall(SYS_mbind, reinterpret_cast<void*>(aligned_begin),
                                  aligned_end - aligned_begin,
                                  MPOL_BIND, mask->maskp, mask->size, 0);
    numa_bitmask_free(mask);
    return result == 0;
}

bool set_preferred(int node) noexcept {
    if (node < 0 || numa_available() < 0) return false;
    numa_set_preferred(node);
    return true;
}

bool pin_current_thread_to_cpu(unsigned cpu) noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>(cpu), &set);
    return ::sched_setaffinity(0, sizeof(set), &set) == 0;
}

int node_of_address(const void* address) noexcept {
    if (address == nullptr || numa_available() < 0) return -1;
    void* pages[1] = {const_cast<void*>(address)};
    int nodes[1] = {-1};
    const long result = ::syscall(SYS_move_pages, 0, 1ul, pages, nullptr, nodes, 0);
    return result == 0 ? nodes[0] : -1;
}

}  // namespace sb::numa
