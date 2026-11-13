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
#include "test_framework.hpp"

#include "sysbench/memory.hpp"
#include "sysbench/options.hpp"

#include <cstring>

using namespace sb;

namespace {

int make_args(char** storage, const char* const* values, int count) {
    for (int i = 0; i < count; ++i) storage[i] = const_cast<char*>(values[i]);
    return count;
}

}  // namespace

TEST("defaults and typed setters") {
    mem::RootArena root(4 * 1024 * 1024);
    mem::HeapArena heap(root, 2 * 1024 * 1024);
    Options options(heap);
    CHECK_EQ(options.as_int("threads"), 1);
    CHECK_EQ(options.as_string("db-driver"), "mysql");
    CHECK_EQ(options.as_size("memory-size"), 4096ull * 1024 * 1024);
    mem::String error{mem::Allocator<char>(&heap)};
    CHECK(options.set("threads", "16", error));
    CHECK_EQ(options.as_int("threads"), 16);
    CHECK(options.set("histogram", "on", error));
    CHECK(options.as_bool("histogram"));
    CHECK(options.set("histogram", "off", error));
    CHECK(!options.as_bool("histogram"));
    CHECK(options.set("memory-size", "1.5G", error));
    CHECK_EQ(options.as_size("memory-size"), 1610612736u);
    CHECK(options.set("mysql-host", "db.example", error));
    CHECK_EQ(options.as_string("mysql-host"), "db.example");
    CHECK(!options.set("threads", "nope", error));
    CHECK(!options.set("missing", "1", error));
}

TEST("first and second pass CLI parsing") {
    mem::RootArena root(4 * 1024 * 1024);
    mem::HeapArena heap(root, 2 * 1024 * 1024);
    Options options(heap);
    const char* values[] = {
        "sysbench", "--threads=4", "--table-size", "1000", "tests/test_options.cpp",
        "--mysql-host", "localhost", "run"};
    char* args[16]{};
    const int argc = make_args(args, values, 8);
    ParsedArgs parsed;
    mem::String error{mem::Allocator<char>(&heap)};
    CHECK(options.scan_arguments(argc, args, parsed, error));
    CHECK_EQ(std::string_view(parsed.script_arg), "tests/test_options.cpp");
    CHECK_EQ(std::string_view(parsed.command), "run");
    CHECK(!options.set("table-size", "1000", error));

    CHECK(options.add("table-size", "Rows per table", OptionType::Int, "10000", true));
    CHECK(options.add("table-size", "duplicate ignored", OptionType::Int, "1", true));
    CHECK_EQ(options.as_int("table-size"), 10000);
    CHECK(options.apply_arguments(argc, args, parsed, error));
    CHECK_EQ(options.as_int("threads"), 4);
    CHECK_EQ(options.as_int("table-size"), 1000);
    CHECK_EQ(options.as_string("mysql-host"), "localhost");
}

TEST("boolean and separate-value forms") {
    mem::RootArena root(4 * 1024 * 1024);
    mem::HeapArena heap(root, 2 * 1024 * 1024);
    Options options(heap);
    const char* values[] = {
        "sysbench", "tests/test_options.cpp", "--histogram", "--time", "30",
        "--report-interval=5", "run"};
    char* args[16]{};
    const int argc = make_args(args, values, 7);
    ParsedArgs parsed;
    mem::String error{mem::Allocator<char>(&heap)};
    CHECK(options.scan_arguments(argc, args, parsed, error));
    CHECK(options.apply_arguments(argc, args, parsed, error));
    CHECK(options.as_bool("histogram"));
    CHECK_EQ(options.as_int("time"), 30);
    CHECK_EQ(options.as_int("report-interval"), 5);
}

TEST("help, version and errors") {
    mem::RootArena root(4 * 1024 * 1024);
    mem::HeapArena heap(root, 2 * 1024 * 1024);
    Options options(heap);
    mem::String error{mem::Allocator<char>(&heap)};
    {
        const char* values[] = {"sysbench", "--help"};
        char* args[4]{};
        ParsedArgs parsed;
        CHECK(options.scan_arguments(make_args(args, values, 2), args, parsed, error));
        CHECK(parsed.help);
    }
    {
        const char* values[] = {"sysbench", "--version"};
        char* args[4]{};
        ParsedArgs parsed;
        CHECK(options.scan_arguments(make_args(args, values, 2), args, parsed, error));
        CHECK(parsed.version);
    }
    {
        const char* values[] = {"sysbench"};
        char* args[4]{};
        ParsedArgs parsed;
        CHECK(!options.scan_arguments(make_args(args, values, 1), args, parsed, error));
    }
    {
        const char* values[] = {"sysbench", "--bad-option=1", "tests/test_options.cpp", "run"};
        char* args[8]{};
        ParsedArgs parsed;
        CHECK(options.scan_arguments(make_args(args, values, 4), args, parsed, error));
        CHECK(!options.apply_arguments(make_args(args, values, 4), args, parsed, error));
    }
}

TEST("option lookup throughput") {
    if (!::sb::test::perf_enabled()) return;
    mem::RootArena root(4 * 1024 * 1024);
    mem::HeapArena heap(root, 2 * 1024 * 1024);
    Options options(heap);
    constexpr std::size_t kOps = 5'000'000;
    std::int64_t total = 0;
    const double ops = ::sb::test::measure_ops_per_second(kOps, [&] {
        for (std::size_t i = 0; i < kOps; ++i) {
            total += options.as_int("threads");
        }
    });
    std::println("  option lookup: {:.1f} Mlookups/s", ops / 1e6);
    CHECK_EQ(total, static_cast<std::int64_t>(kOps));
    CHECK(ops > 1'000'000.0);
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(256ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
