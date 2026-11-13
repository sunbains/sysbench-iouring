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

#include "sysbench/db.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/options.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/stats.hpp"

#include <cstdint>
#include <string_view>

namespace sb {

// Async-signal-safe cooperative stop request (for SIGINT).
void request_lua_stop() noexcept;

struct LuaRunOptions {
    db::ConnectionOptions database;
    std::uint32_t threads = 1;
    std::uint32_t workers = 0;
    std::uint64_t time_seconds = 10;
    std::uint64_t events = 0;
    std::uint64_t warmup_seconds = 0;
    std::uint64_t rate = 0;
    std::uint32_t report_interval = 1;
    std::uint32_t percentile = 95;
    bool histogram = false;
    std::uint64_t seed = 0;
};

// Owns the script's bootstrap Lua state and runs benchmark/command states on
// the runtime.  The bootstrap state is used for option discovery and reporting
// hooks; worker states are created on their owning worker and use that worker's
// arena for every Lua allocation.
class LuaEngine {
public:
    LuaEngine(mem::MemoryResource& resource, Options& options,
              std::string_view script_path, std::string_view command,
              int argc, char** argv);
    ~LuaEngine();

    LuaEngine(const LuaEngine&) = delete;
    LuaEngine& operator=(const LuaEngine&) = delete;

    bool load(mem::String& error);
    bool collect_options(mem::String& error);
    bool install_options(mem::String& error);
    [[nodiscard]] bool command_parallel(std::string_view command);
    [[nodiscard]] bool has_global(std::string_view name);

    // Runs the benchmark until its time/event limit, printing periodic and final
    // reports.  Returns 0 on success.
    int run(Runtime& runtime, const LuaRunOptions& run_options);

    // Runs prepare/cleanup/custom commands.  parallel=true executes one state per
    // --threads; otherwise a single state.
    int run_command(Runtime& runtime, const LuaRunOptions& run_options,
                    std::string_view command, bool parallel);

    int call_help();

    void print_help() const;

private:
    struct Impl;
    Impl* m_impl;
};

}  // namespace sb
