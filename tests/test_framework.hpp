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

#include <print>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <limits>
#include <string_view>
#include <utility>

namespace sb::test {

struct TestCase {
    const char* name;
    void (*function)();
};

inline TestCase* registry() {
    static TestCase cases[256]{};
    return cases;
}

inline int& registry_size() {
    static int size = 0;
    return size;
}

struct Registrar {
    Registrar(const char* name, void (*function)()) {
        const int index = registry_size()++;
        registry()[index] = TestCase{name, function};
    }
};

inline std::atomic<int>& failures() {
    static std::atomic<int> count{0};
    return count;
}

inline std::string_view& current_test() {
    static std::string_view name;
    return name;
}

inline void fail(const char* expression, const char* file, int line,
                 std::string_view message = {}) {
    failures().fetch_add(1, std::memory_order_relaxed);
    std::println(stderr, "  FAIL {}:{}: {}", file, line, expression);
    if (!message.empty()) std::println(stderr, " ({})", message.data());
    std::fputc('\n', stderr);
}

template <class A, class B>
void check_eq(const A& a, const B& b, const char* ea, const char* eb,
              const char* file, int line) {
    if (!(a == b)) {
        failures().fetch_add(1, std::memory_order_relaxed);
        std::println(stderr, "  FAIL {}:{}: {} == {}", file, line, ea, eb);
    }
}

inline bool perf_enabled() {
    const char* value = std::getenv("SYSBENCH_SKIP_PERF");
    return value == nullptr || std::string_view(value) != "1";
}

template <class Fn>
double measure_ops_per_second(std::size_t operations, Fn&& fn) {
    const auto start = std::chrono::steady_clock::now();
    std::forward<Fn>(fn)();
    const auto end = std::chrono::steady_clock::now();
    const double seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
            .count();
    return seconds > 0.0 ? static_cast<double>(operations) / seconds
                         : std::numeric_limits<double>::infinity();
}

inline int run_all(int argc, char** argv) {
    std::string_view filter;
    if (argc > 1) filter = argv[1];
    int ran = 0;
    for (int i = 0; i < registry_size(); ++i) {
        const TestCase& test = registry()[i];
        if (!filter.empty() && std::string_view(test.name).find(filter) ==
                                   std::string_view::npos) {
            continue;
        }
        current_test() = test.name;
        const int before = failures().load(std::memory_order_relaxed);
        std::println("[ RUN      ] {}", test.name);
        try {
            test.function();
        } catch (const std::exception& error) {
            failures().fetch_add(1, std::memory_order_relaxed);
            std::println(stderr, "  FAIL exception: {}", error.what());
        } catch (...) {
            failures().fetch_add(1, std::memory_order_relaxed);
            std::println(stderr, "  FAIL unknown exception");
        }
        const bool ok = failures().load(std::memory_order_relaxed) == before;
        std::println("[ {} ] {}", ok ? "       OK" : "  FAILED", test.name);
        ++ran;
    }
    const int failure_count = failures().load(std::memory_order_relaxed);
    std::println("{} test(s) ran, {} failure(s)", ran, failure_count);
    return failure_count == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

}  // namespace sb::test

#define SB_CONCAT_IMPL(a, b) a##b
#define SB_CONCAT(a, b) SB_CONCAT_IMPL(a, b)
#define TEST(name)                                                     \
    static void SB_CONCAT(m_sb_test, __LINE__)();                       \
    static ::sb::test::Registrar SB_CONCAT(m_sb_registrar, __LINE__)(   \
        name, &SB_CONCAT(m_sb_test, __LINE__));                         \
    static void SB_CONCAT(m_sb_test, __LINE__)()

#define CHECK(expr) \
    do { if (!(expr)) ::sb::test::fail(#expr, __FILE__, __LINE__); } while (0)
#define REQUIRE(expr)                                                    \
    do {                                                                 \
        if (!(expr)) {                                                   \
            ::sb::test::fail(#expr, __FILE__, __LINE__);                 \
            return;                                                      \
        }                                                                \
    } while (0)
#define CHECK_EQ(a, b) \
    ::sb::test::check_eq((a), (b), #a, #b, __FILE__, __LINE__)
#define CHECK_NE(a, b) CHECK((a) != (b))
#define CHECK_THROWS(expr)                                               \
    do {                                                                 \
        bool sb_threw = false;                                           \
        try { (void)(expr); } catch (...) { sb_threw = true; }           \
        if (!sb_threw) ::sb::test::fail(#expr " throws", __FILE__, __LINE__); \
    } while (0)
#define CHECK_NOTHROW(expr)                                              \
    do {                                                                 \
        try { (void)(expr); }                                            \
        catch (...) { ::sb::test::fail(#expr " does not throw", __FILE__, __LINE__); } \
    } while (0)
#define TEST_MAIN() int main(int argc, char** argv) { return ::sb::test::run_all(argc, argv); }
