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

// Per-layer cost accounting (build with -DSYSBENCH_LAYER_STATS=ON).
//
// Each worker thread keeps thread-local TSC totals per layer.  A LayerScope
// charges the time spent inside it to its layer *exclusively*: entering a
// nested scope stops the clock of the enclosing one.  Event counters (bytes
// copied, syscalls, SQEs, CQEs) sit beside them.  Nothing is shared while the
// benchmark runs; the totals are summed when the workers have stopped.
// Without the option every hook compiles to nothing.

#include <cstdint>

namespace sb::layer {

enum class Layer : std::uint8_t {
    Scheduler,     // worker loop: ready queue, timers, bookkeeping
    Reap,          // walking the CQ, resuming tasks
    Coroutine,     // task code outside every finer scope (driver/glue code)
    Lua,           // the Lua VM running the script
    LuaBinding,    // C functions called from Lua (sb_sql_*, rand, params, ...)
    Dispatch,      // perform_pending: turning a Lua request into a driver call
    Protocol,      // MySQL framing and reply parsing
    IoPrep,        // staging sends, preparing SQEs
    KernelSubmit,  // io_uring_enter without waiting (submit + run task work)
    KernelWait,    // io_uring_enter waiting for a completion (incl. busy poll)
    Idle,          // futex wait with nothing in flight
    Memory,        // coroutine frame allocation and release
    Count
};

enum class Counter : std::uint8_t {
    Queries,         // replies read (one per query)
    CopyIn,          // bytes copied out of the read buffer (packet payloads)
    CompactBytes,    // bytes moved to the front of the read buffer
    CopyOut,         // bytes framed into the write buffer
    SubmitCalls,     // non-waiting io_uring_enter calls
    WaitCalls,       // waiting io_uring_enter calls
    Sqes,            // SQEs prepared
    Cqes,            // CQEs reaped
    Resumes,         // task resumes
    Scopes,          // layer scopes entered (instrumentation overhead)
    Count
};

#if defined(SYSBENCH_LAYER_STATS)

inline constexpr bool kEnabled = true;

struct ThreadTotals {
    std::uint64_t m_cycles[static_cast<unsigned>(Layer::Count)]{};
    std::uint64_t m_counts[static_cast<unsigned>(Counter::Count)]{};
    Layer m_current = Layer::Scheduler;
    std::uint64_t m_since = 0;
    bool m_registered = false;
};

ThreadTotals& totals() noexcept;
void register_thread() noexcept;
// Print the per-query breakdown summed over every registered thread.
void report(double cpu_ghz_hint = 0.0);

[[nodiscard]] inline std::uint64_t now() noexcept { return __builtin_ia32_rdtsc(); }

inline void count(Counter counter, std::uint64_t value = 1) noexcept {
    totals().m_counts[static_cast<unsigned>(counter)] += value;
}

class LayerScope {
public:
    explicit LayerScope(Layer layer) noexcept : m_totals(&totals()) {
        const std::uint64_t t = now();
        ++m_totals->m_counts[static_cast<unsigned>(Counter::Scopes)];
        m_saved = m_totals->m_current;
        m_totals->m_cycles[static_cast<unsigned>(m_saved)] += t - m_totals->m_since;
        m_totals->m_current = layer;
        m_totals->m_since = t;
    }
    ~LayerScope() {
        const std::uint64_t t = now();
        m_totals->m_cycles[static_cast<unsigned>(m_totals->m_current)] += t - m_totals->m_since;
        m_totals->m_current = m_saved;
        m_totals->m_since = t;
    }
    LayerScope(const LayerScope&) = delete;
    LayerScope& operator=(const LayerScope&) = delete;

private:
    ThreadTotals* m_totals;
    Layer m_saved;
};

#else

inline constexpr bool kEnabled = false;
inline void register_thread() noexcept {}
inline void report(double = 0.0) {}
inline void count(Counter, std::uint64_t = 1) noexcept {}
class LayerScope {
public:
    explicit LayerScope(Layer) noexcept {}
};

#endif

}  // namespace sb::layer

#define SB_LAYER_CAT2(a, b) a##b
#define SB_LAYER_CAT(a, b) SB_LAYER_CAT2(a, b)
// Charge the rest of the enclosing block to `layer`.
#define SB_LAYER(which) \
    ::sb::layer::LayerScope SB_LAYER_CAT(sb_layer_scope_, __LINE__)(::sb::layer::Layer::which)
#define SB_COUNT(which, value) ::sb::layer::count(::sb::layer::Counter::which, (value))
