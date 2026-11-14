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

#include "sysbench/layer_stats.hpp"

#if defined(SYSBENCH_LAYER_STATS)

#include <atomic>
#include <chrono>
#include <cstdio>

namespace sb::layer {
namespace {

constexpr unsigned kMaxThreads = 256;
// Registration happens once per thread at start-up; the totals themselves
// are thread-local and never shared while the run is going.
std::atomic<unsigned> g_thread_count{0};
ThreadTotals* g_threads[kMaxThreads]{};
std::atomic<std::uint64_t> g_start_tsc{0};
std::atomic<std::int64_t> g_start_ns{0};
// Cycles one LayerScope adds (measured once): reported as overhead.
std::atomic<std::uint64_t> g_scope_cycles{0};

thread_local ThreadTotals tls_totals;

constexpr const char* kLayerNames[] = {
    "scheduler (worker loop)", "reap + resume",    "coroutine/driver glue",
    "Lua VM (script)",         "Lua C bindings",   "dispatch",
    "MySQL protocol",          "SQE prep / staging", "kernel: submit",
    "kernel: wait (+busy poll)", "idle (futex)", "coroutine frames"};
constexpr const char* kCounterNames[] = {
    "queries",        "bytes copied in", "bytes compacted", "bytes framed out",
    "submit syscalls", "wait syscalls",  "SQEs",            "CQEs",
    "task resumes",   "layer scopes"};
static_assert(sizeof(kLayerNames) / sizeof(kLayerNames[0]) ==
              static_cast<unsigned>(Layer::Count));
static_assert(sizeof(kCounterNames) / sizeof(kCounterNames[0]) ==
              static_cast<unsigned>(Counter::Count));

}  // namespace

ThreadTotals& totals() noexcept { return tls_totals; }

void register_thread() noexcept {
    ThreadTotals& mine = tls_totals;
    if (mine.m_registered) return;
    mine.m_registered = true;
    mine.m_current = Layer::Scheduler;
    mine.m_since = now();
    std::uint64_t expected = 0;
    if (g_start_tsc.compare_exchange_strong(expected, mine.m_since)) {
        g_start_ns.store(std::chrono::steady_clock::now().time_since_epoch().count());
    }
    if (g_scope_cycles.load() == 0) {
        constexpr unsigned kLoops = 4096;
        const std::uint64_t begin = now();
        for (unsigned i = 0; i < kLoops; ++i) {
            LayerScope scope(Layer::Scheduler);
            asm volatile("" ::: "memory");
        }
        g_scope_cycles.store((now() - begin) / kLoops + 1);
        mine.m_counts[static_cast<unsigned>(Counter::Scopes)] = 0;
    }
    const unsigned slot = g_thread_count.fetch_add(1);
    if (slot < kMaxThreads) g_threads[slot] = &mine;
}

void report(double) {
    const unsigned threads = std::min(g_thread_count.load(), kMaxThreads);
    if (threads == 0) return;
    std::uint64_t cycles[static_cast<unsigned>(Layer::Count)]{};
    std::uint64_t counts[static_cast<unsigned>(Counter::Count)]{};
    for (unsigned t = 0; t < threads; ++t) {
        for (unsigned i = 0; i < static_cast<unsigned>(Layer::Count); ++i)
            cycles[i] += g_threads[t]->m_cycles[i];
        for (unsigned i = 0; i < static_cast<unsigned>(Counter::Count); ++i)
            counts[i] += g_threads[t]->m_counts[i];
    }
    const double elapsed_ns = static_cast<double>(
        std::chrono::steady_clock::now().time_since_epoch().count() - g_start_ns.load());
    const double tsc_per_ns =
        elapsed_ns > 0 ? static_cast<double>(now() - g_start_tsc.load()) / elapsed_ns : 1.0;
    const double queries = static_cast<double>(
        counts[static_cast<unsigned>(Counter::Queries)] ? counts[static_cast<unsigned>(Counter::Queries)] : 1);
    std::uint64_t busy = 0;
    for (unsigned i = 0; i < static_cast<unsigned>(Layer::Count); ++i) {
        if (i != static_cast<unsigned>(Layer::KernelWait) && i != static_cast<unsigned>(Layer::Idle))
            busy += cycles[i];
    }
    std::printf("\nPer-layer cost (%u worker threads, TSC %.2f GHz), per query:\n", threads,
                tsc_per_ns);
    for (unsigned i = 0; i < static_cast<unsigned>(Layer::Count); ++i) {
        const double per_query = static_cast<double>(cycles[i]) / queries;
        std::printf("    %-28s %9.0f cycles %8.3f us", kLayerNames[i], per_query,
                    per_query / tsc_per_ns / 1000.0);
        if (i != static_cast<unsigned>(Layer::KernelWait) && i != static_cast<unsigned>(Layer::Idle))
            std::printf("  %5.1f%% of busy", 100.0 * static_cast<double>(cycles[i]) /
                                                 static_cast<double>(busy ? busy : 1));
        std::printf("\n");
    }
    std::printf("    %-28s %9.0f cycles %8.3f us\n", "= busy (excl. wait/idle)",
                static_cast<double>(busy) / queries,
                static_cast<double>(busy) / queries / tsc_per_ns / 1000.0);
    for (unsigned i = 1; i < static_cast<unsigned>(Counter::Count); ++i) {
        std::printf("    %-28s %9.2f\n", kCounterNames[i], static_cast<double>(counts[i]) / queries);
    }
    std::printf("    %-28s %9.0f\n", "queries counted", queries);
    const double scopes = static_cast<double>(counts[static_cast<unsigned>(Counter::Scopes)]) / queries;
    std::printf("    instrumentation: %.1f scopes/query x %llu cycles = %.0f cycles/query (%.3f us) "
                "included above\n",
                scopes, static_cast<unsigned long long>(g_scope_cycles.load()),
                scopes * static_cast<double>(g_scope_cycles.load()),
                scopes * static_cast<double>(g_scope_cycles.load()) / tsc_per_ns / 1000.0);
}

}  // namespace sb::layer

#endif
