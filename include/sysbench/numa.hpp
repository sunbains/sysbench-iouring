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

#pragma once

// NUMA topology discovery and node-local placement helpers.  The project only
// uses libnuma for topology/binding; all service memory still comes from the
// root arena.

#include <cstddef>
#include <cstdint>
#include <array>

#include <sched.h>

namespace sb::numa {

inline constexpr std::uint32_t kMaxNodes = 64;
inline constexpr std::uint32_t kMaxCpuCount = 4096;
inline constexpr std::uint32_t kMaxNodeCpus = 1024;

struct NodeInfo {
    int m_id = -1;
    std::uint32_t m_cpu_count = 0;
    std::array<std::uint32_t, kMaxNodeCpus> m_cpus{};
};

class Topology {
public:
    static const Topology& instance();

    [[nodiscard]] bool available() const noexcept { return m_available; }
    [[nodiscard]] std::uint32_t node_count() const noexcept { return m_node_count; }
    [[nodiscard]] const NodeInfo& node(std::uint32_t index) const noexcept;
    [[nodiscard]] const NodeInfo* node_by_id(int id) const noexcept;
    [[nodiscard]] int home_node() const noexcept { return m_home_node; }
    [[nodiscard]] int reserved_cpu() const noexcept { return m_reserved_cpu; }
    /* The CPUs the process was allowed at startup, before the main thread
    pinned itself to the reserved CPU (threads inherit their creator's mask,
    so an unpinned worker must restore this one). */
    [[nodiscard]] const cpu_set_t& process_cpus() const noexcept { return m_process_cpus; }
    [[nodiscard]] int node_of_cpu(unsigned cpu) const noexcept;

    // Worker assignment is balanced round-robin across online nodes.  CPUS are
    // ordered so that one thread from each physical core is used before SMT
    // siblings.
    [[nodiscard]] int node_for_worker(std::uint32_t worker) const noexcept;
    [[nodiscard]] int cpu_for_worker(std::uint32_t worker) const noexcept;

private:
    Topology();

    std::array<NodeInfo, kMaxNodes> m_nodes{};
    std::array<int, kMaxCpuCount> m_cpu_to_node{};
    std::uint32_t m_node_count = 0;
    int m_home_node = 0;
    int m_reserved_cpu = -1;
    cpu_set_t m_process_cpus{};
    bool m_available = false;
};

// Bind a page-aligned root-arena range to one NUMA node before first touch.
// Returns false when NUMA is unavailable or mbind is not permitted.
bool bind_range(void* address, std::size_t length, int node) noexcept;

// Set the current thread's preferred allocation node.
bool set_preferred(int node) noexcept;
bool pin_current_thread_to_cpu(unsigned cpu) noexcept;

// Ask the kernel on which node a touched page currently resides. -1 on error.
int node_of_address(const void* address) noexcept;

}  // namespace sb::numa
