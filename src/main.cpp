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

#include <print>
#include "sysbench/lua.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/mysql.hpp"
#include "sysbench/numa.hpp"
#include "sysbench/options.hpp"
#include "sysbench/runtime.hpp"

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

namespace {

void handle_signal(int) {
    sb::request_lua_stop();
}

void print_error(sb::mem::String& error) {
    if (!error.empty()) std::println(stderr, "sysbench: {}", error.c_str());
}

void print_version() {
    std::println("sysbench-cpp {} (C++26, io_uring, Lua 5.4)", SYSBENCH_VERSION);
}

void print_builtin_help(sb::Options& options) {
    std::println("Usage: sysbench [options] SCRIPT COMMAND\n");
    std::println("Commands:");
    std::println("  run       Execute the benchmark");
    std::println("  prepare   Create the benchmark dataset");
    std::println("  cleanup   Drop the benchmark dataset");
    std::println("  warmup    Warm up the database cache");
    std::println("  help      Show benchmark-specific help\n");
    std::println("Options:");
    for (std::size_t i = 0; i < options.size(); ++i) {
        const sb::OptionDef* option = options.at(i);
        if (option == nullptr) continue;
        char display[192]{};
        const std::size_t length =
            std::min(option->m_name.size(), sizeof(display) - 1);
        for (std::size_t j = 0; j < length; ++j) {
            display[j] = option->m_name[j] == '_' ? '-' : option->m_name[j];
        }
        std::println("  --{:<28} {} (default: {})", display,
                    option->m_description.c_str(), option->m_value.c_str());
    }
}

struct MemoryDefaults {
    std::uint64_t m_bytes = 4ull * 1024 * 1024 * 1024;
    bool m_populate = false;
};

MemoryDefaults prescan_memory(int argc, char** argv) {
    MemoryDefaults result;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        if (argument == "--populate-memory") {
            result.m_populate = true;
        } else if (argument.starts_with("--memory-size=")) {
            std::uint64_t parsed = 0;
            if (sb::Options::parse_size_value(argument.substr(14), parsed) &&
                parsed > 0) {
                result.m_bytes = parsed;
            }
        } else if (argument == "--memory-size" && i + 1 < argc) {
            std::uint64_t parsed = 0;
            if (sb::Options::parse_size_value(argv[i + 1], parsed) && parsed > 0) {
                result.m_bytes = parsed;
            }
            ++i;
        }
    }
    if (result.m_bytes < 64ull * 1024 * 1024) result.m_bytes = 64ull * 1024 * 1024;
    return result;
}

std::uint64_t choose_worker_heap(std::uint64_t root_capacity,
                                 std::uint64_t main_heap,
                                 std::uint32_t workers,
                                 std::uint64_t buffer_bytes) {
    constexpr std::uint64_t minimum = 4ull * 1024 * 1024;
    constexpr std::uint64_t maximum = 64ull * 1024 * 1024;
    if (root_capacity <= main_heap + buffer_bytes + minimum) return minimum;
    const std::uint64_t available =
        (root_capacity - main_heap - buffer_bytes) * 8 / 10;
    const std::uint64_t per_worker = available / workers;
    return std::clamp<std::uint64_t>(per_worker, minimum, maximum);
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    const MemoryDefaults memory = prescan_memory(argc, argv);
    try {
        sb::mem::GlobalAllocator::instance().initialize(memory.m_bytes, memory.m_populate);
    } catch (const std::exception& error) {
        std::println(stderr, "sysbench: cannot reserve {} bytes: {}",
                     static_cast<unsigned long long>(memory.m_bytes), error.what());
        return EXIT_FAILURE;
    }

    const auto& numa_topology = sb::numa::Topology::instance();
    const int home_node = numa_topology.available() ? numa_topology.home_node() : -1;
    if (home_node >= 0) {
        (void)sb::numa::set_preferred(home_node);
        sb::mem::set_thread_frame_node(home_node);
    }
    const int reserved_cpu = numa_topology.reserved_cpu();
    if (reserved_cpu >= 0) {
        (void)sb::numa::pin_current_thread_to_cpu(
            static_cast<unsigned>(reserved_cpu));
    }
    auto& root = sb::mem::GlobalAllocator::instance().root();
    const std::uint64_t main_heap_bytes =
        std::min<std::uint64_t>(256ull * 1024 * 1024,
                                std::max<std::uint64_t>(16ull * 1024 * 1024,
                                                        memory.m_bytes / 16));
    sb::mem::HeapArena main_heap(root, static_cast<std::size_t>(main_heap_bytes),
                                 "main", home_node);
    sb::Options options(main_heap);

    sb::ParsedArgs parsed;
    sb::mem::String error{sb::mem::Allocator<char>(&main_heap)};
    if (!options.scan_arguments(argc, argv, parsed, error)) {
        print_error(error);
        print_builtin_help(options);
        return EXIT_FAILURE;
    }
    if (parsed.m_version) {
        print_version();
        return EXIT_SUCCESS;
    }
    if (parsed.m_help && parsed.m_script_path.empty()) {
        print_builtin_help(options);
        return EXIT_SUCCESS;
    }

    sb::mysql::register_driver();

    auto engine = sb::mem::construct<sb::LuaEngine>(
        main_heap, main_heap, options, std::string_view(parsed.m_script_path),
        std::string_view(parsed.m_command), argc, argv);
    if (!engine->load(error)) {
        print_error(error);
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }
    if (!engine->collect_options(error)) {
        print_error(error);
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }
    if (parsed.m_help) {
        engine->print_help();
        sb::mem::destroy(main_heap, engine);
        return EXIT_SUCCESS;
    }
    if (!options.apply_arguments(argc, argv, parsed, error)) {
        print_error(error);
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }
    if (!engine->install_options(error)) {
        print_error(error);
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }

    const std::uint32_t threads = static_cast<std::uint32_t>(
        std::max<std::int64_t>(1, options.as_int("threads", 1)));
    const std::uint64_t time_seconds =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, options.as_int("time", 10)));
    const std::uint64_t events =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, options.as_int("events", 0)));
    if (parsed.m_command == "run" && time_seconds == 0 && events == 0) {
        std::println(stderr, "sysbench: either --time or --events must be non-zero");
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }

    const std::uint32_t workers_setting = static_cast<std::uint32_t>(
        std::max<std::int64_t>(0, options.as_int("workers", 0)));
    std::uint32_t workers =
        workers_setting != 0
            ? workers_setting
            : std::min<std::uint32_t>(threads, sb::Runtime::hardware_workers());
    if (workers > sb::Runtime::kMaxWorkers) {
        std::println(stderr, "sysbench: --workers capped at {}",
                     sb::Runtime::kMaxWorkers);
        workers = sb::Runtime::kMaxWorkers;
    }
    const std::uint64_t io_bytes =
        static_cast<std::uint64_t>(threads) * 96 * 1024;
    const std::uint64_t worker_heap_bytes =
        choose_worker_heap(root.capacity(), main_heap_bytes, workers, io_bytes);
    const std::uint64_t required =
        main_heap_bytes + worker_heap_bytes * workers + io_bytes;
    if (required > root.capacity() * 9 / 10) {
        std::println(stderr,
                     "sysbench: --memory-size is too small for {} worker(s) and "
                     "{} thread(s); need at least {:.1f} GiB",
                     workers, threads,
                     static_cast<double>(required) * 1.25 /
                         (1024.0 * 1024.0 * 1024.0));
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }

    sb::db::ConnectionOptions database;
    const auto host = options.as_string("mysql-host", "127.0.0.1");
    const auto port = options.as_string("mysql-port", "3306");
    const auto user = options.as_string("mysql-user", "root");
    const auto password = options.as_string("mysql-password", "");
    const auto database_name = options.as_string("mysql-db", "");
    const auto socket_path = options.as_string("mysql-socket", "");
    database.m_host = host;
    database.m_port = port;
    database.m_user = user;
    database.m_password = password;
    database.m_database = database_name;
    database.m_socket_path = socket_path;
    database.m_connect_timeout_ms = 5000;
    const auto ssl_mode = options.as_string("mysql-ssl", "off");
    if (ssl_mode == "on" || ssl_mode == "required") {
        database.m_tls_mode = sb::db::TlsMode::Required;
    } else if (ssl_mode == "preferred") {
        database.m_tls_mode = sb::db::TlsMode::Preferred;
    } else if (ssl_mode == "off" || ssl_mode == "disabled") {
        database.m_tls_mode = sb::db::TlsMode::Disabled;
    } else {
        std::println(stderr, "sysbench: --mysql-ssl must be off, preferred, or required");
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }
    database.m_tls_verify_server_cert =
        options.as_bool("mysql-ssl-verify-server-cert", false);
    database.m_tls_ca = options.as_string("mysql-ssl-ca", "");
    database.m_tls_cert = options.as_string("mysql-ssl-cert", "");
    database.m_tls_key = options.as_string("mysql-ssl-key", "");

    sb::LuaRunOptions run_options;
    run_options.m_database = database;
    run_options.m_threads = threads;
    run_options.m_workers = workers;
    run_options.m_time_seconds = time_seconds;
    run_options.m_events = events;
    run_options.m_warmup_seconds =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, options.as_int("warmup-time", 0)));
    run_options.m_rate =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, options.as_int("rate", 0)));
    run_options.m_report_interval = static_cast<std::uint32_t>(
        std::max<std::int64_t>(0, options.as_int("report-interval", 1)));
    run_options.m_percentile = static_cast<std::uint32_t>(
        std::clamp<std::int64_t>(options.as_int("percentile", 95), 0, 100));
    run_options.m_histogram = options.as_bool("histogram", false);
    const std::int64_t seed = options.as_int("seed", 0) != 0
                                  ? options.as_int("seed", 0)
                                  : options.as_int("rand-seed", 0);
    run_options.m_seed = static_cast<std::uint64_t>(std::max<std::int64_t>(0, seed));
    const bool numa_enabled = options.as_bool("numa", true);
    const bool pin_workers = options.as_bool("pin-workers", true);

    if (parsed.m_command == "help") {
        const int result = engine->call_help();
        sb::mem::destroy(main_heap, engine);
        return result;
    }

    sb::Runtime runtime;
    try {
        runtime.start(sb::Runtime::Config{
            .m_workers = workers,
            .m_worker_heap_bytes = static_cast<std::size_t>(worker_heap_bytes),
            .m_pin_workers = pin_workers,
            .m_numa_aware = numa_enabled,
            .m_first_cpu = 0});
    } catch (const std::exception& failure) {
        std::println(stderr, "sysbench: cannot start runtime: {}", failure.what());
        sb::mem::destroy(main_heap, engine);
        return EXIT_FAILURE;
    }

    int result = EXIT_SUCCESS;
    if (parsed.m_command == "run") {
        std::println("sysbench {}: running {} with {} thread(s) on {} worker(s), "
                     "{} NUMA node(s){}",
                     SYSBENCH_VERSION, parsed.m_script_arg.c_str(), threads, workers,
                     numa_topology.node_count(),
                     numa_enabled && numa_topology.available() ? " (node-local)" : "");
        result = engine->run(runtime, run_options);
    } else {
        const bool parallel = engine->command_parallel(parsed.m_command);
        std::println("sysbench {}: executing '{}' with {} thread(s)",
                    SYSBENCH_VERSION, parsed.m_command.c_str(),
                    parallel ? threads : 1u);
        result = engine->run_command(runtime, run_options, parsed.m_command, parallel);
    }

    runtime.shutdown();
    sb::mem::destroy(main_heap, engine);
    return result;
}
