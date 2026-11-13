#include "sysbench/stats.hpp"

#include <algorithm>
#include <bit>
#include <cctype>
#include <cmath>

namespace sb {
namespace {

constexpr unsigned kSubBins = 8;
constexpr unsigned kBaseExponent = 10;  // 2^10 ns ~= 1 us

std::size_t latency_bin(std::uint64_t nanoseconds) {
    if (nanoseconds <= LatencyHistogram::kMinNs) return 0;
    if (nanoseconds >= LatencyHistogram::kMaxNs) return LatencyHistogram::kBins - 1;
    const unsigned exponent =
        static_cast<unsigned>(std::bit_width(nanoseconds) - 1);
    if (exponent < kBaseExponent) return 0;
    const std::uint64_t base = std::uint64_t{1} << exponent;
    const unsigned sub = static_cast<unsigned>(
        ((nanoseconds - base) * kSubBins) >> exponent);
    const std::size_t index =
        1 + (exponent - kBaseExponent) * kSubBins + std::min(sub, kSubBins - 1);
    return std::min<std::size_t>(index, LatencyHistogram::kBins - 1);
}

double latency_upper_bound(std::size_t bin) {
    if (bin == 0) return static_cast<double>(LatencyHistogram::kMinNs);
    const std::size_t zero_based = bin - 1;
    const unsigned exponent =
        kBaseExponent + static_cast<unsigned>(zero_based / kSubBins);
    const unsigned sub = static_cast<unsigned>(zero_based % kSubBins);
    const double base = std::ldexp(1.0, static_cast<int>(exponent));
    return base * (1.0 + static_cast<double>(sub + 1) / kSubBins);
}

double percentile_from_counts(const std::array<std::uint64_t, LatencyHistogram::kBins>& counts,
                              std::uint64_t total, double fraction) {
    if (total == 0) return 0.0;
    fraction = std::clamp(fraction, 0.0, 1.0);
    const auto target = static_cast<std::uint64_t>(
        std::ceil(fraction * static_cast<double>(total)));
    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < counts.size(); ++i) {
        cumulative += counts[i];
        if (cumulative >= target) return latency_upper_bound(i) * 1e-9;
    }
    return latency_upper_bound(LatencyHistogram::kBins - 1) * 1e-9;
}

std::string_view trim_left(std::string_view value) {
    while (!value.empty() &&
           (std::isspace(static_cast<unsigned char>(value.front())) != 0 ||
            value.front() == '(')) {
        value.remove_prefix(1);
    }
    return value;
}

char upper(char value) {
    return static_cast<char>(std::toupper(static_cast<unsigned char>(value)));
}

bool starts_with_ci(std::string_view value, std::string_view prefix) {
    if (value.size() < prefix.size()) return false;
    for (std::size_t i = 0; i < prefix.size(); ++i) {
        if (upper(value[i]) != upper(prefix[i])) return false;
    }
    return true;
}

}  // namespace

QueryKind classify_query(std::string_view sql) noexcept {
    sql = trim_left(sql);
    // Skip line and block comments without allocating.
    for (;;) {
        if (sql.size() >= 2 && sql[0] == '/' && sql[1] == '*') {
            const auto end = sql.find("*/", 2);
            if (end == std::string_view::npos) return QueryKind::Other;
            sql = trim_left(sql.substr(end + 2));
            continue;
        }
        if (sql.size() >= 2 && sql[0] == '-' && sql[1] == '-') {
            const auto end = sql.find('\n');
            if (end == std::string_view::npos) return QueryKind::Other;
            sql = trim_left(sql.substr(end + 1));
            continue;
        }
        break;
    }
    if (starts_with_ci(sql, "SELECT") || starts_with_ci(sql, "SHOW") ||
        starts_with_ci(sql, "DESC") || starts_with_ci(sql, "EXPLAIN") ||
        starts_with_ci(sql, "WITH")) {
        return QueryKind::Read;
    }
    if (starts_with_ci(sql, "INSERT") || starts_with_ci(sql, "UPDATE") ||
        starts_with_ci(sql, "DELETE") || starts_with_ci(sql, "REPLACE") ||
        starts_with_ci(sql, "ALTER") || starts_with_ci(sql, "CREATE") ||
        starts_with_ci(sql, "DROP") || starts_with_ci(sql, "TRUNCATE")) {
        return QueryKind::Write;
    }
    if (starts_with_ci(sql, "BEGIN") || starts_with_ci(sql, "START") ||
        starts_with_ci(sql, "COMMIT") || starts_with_ci(sql, "ROLLBACK") ||
        starts_with_ci(sql, "SET")) {
        return QueryKind::Transaction;
    }
    return QueryKind::Other;
}

void LatencyHistogram::record(std::uint64_t nanoseconds) noexcept {
    m_bins[bin_for(nanoseconds)].fetch_add(1, std::memory_order_relaxed);
}

std::size_t LatencyHistogram::bin_for(std::uint64_t nanoseconds) noexcept {
    return latency_bin(nanoseconds);
}

double LatencyHistogram::upper_bound(std::size_t bin) noexcept {
    return latency_upper_bound(bin);
}

double LatencyHistogram::bin_upper_bound_seconds(std::size_t bin) noexcept {
    return latency_upper_bound(std::min(bin, kBins - 1)) * 1e-9;
}

double LatencyHistogram::percentile(double fraction) const {
    const Snapshot current = snapshot();
    return percentile_from_counts(current.counts, current.total, fraction);
}

std::uint64_t LatencyHistogram::observations() const noexcept {
    std::uint64_t total = 0;
    for (const auto& bin : m_bins) total += bin.load(std::memory_order_relaxed);
    return total;
}

void LatencyHistogram::reset() noexcept {
    for (auto& bin : m_bins) bin.store(0, std::memory_order_relaxed);
}

void LatencyHistogram::merge(const LatencyHistogram& other) noexcept {
    const Snapshot snapshot = other.snapshot();
    for (std::size_t i = 0; i < m_bins.size(); ++i) {
        m_bins[i].fetch_add(snapshot.counts[i], std::memory_order_relaxed);
    }
}

LatencyHistogram::Snapshot LatencyHistogram::snapshot() const noexcept {
    Snapshot result;
    for (std::size_t i = 0; i < m_bins.size(); ++i) {
        result.counts[i] = m_bins[i].load(std::memory_order_relaxed);
        result.total += result.counts[i];
    }
    return result;
}

ThreadStats::ThreadStats(std::uint32_t thread_id) noexcept
    : m_thread_id(thread_id) {}

void ThreadStats::begin_event(std::chrono::steady_clock::time_point start) noexcept {
    m_event_start = start;
}

void ThreadStats::end_event(std::chrono::steady_clock::time_point end,
                            bool error) noexcept {
    const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             end - m_event_start)
                             .count();
    record_event_latency(latency < 0 ? 0 : static_cast<std::uint64_t>(latency), error);
}

void ThreadStats::record_event_latency(std::uint64_t nanoseconds,
                                       bool error) noexcept {
    m_events.fetch_add(1, std::memory_order_relaxed);
    m_latency_sum_ns.fetch_add(nanoseconds, std::memory_order_relaxed);
    m_latency.record(nanoseconds);
    if (nanoseconds > m_max_local) {
        m_max_local = nanoseconds;
        m_latency_max_ns.store(nanoseconds, std::memory_order_relaxed);
    }
    if (nanoseconds < m_min_local) {
        m_min_local = nanoseconds;
        m_latency_min_ns.store(nanoseconds, std::memory_order_relaxed);
    }
    if (error) m_errors.fetch_add(1, std::memory_order_relaxed);
}

void ThreadStats::record_query(QueryKind kind, std::uint64_t bytes_read,
                               std::uint64_t bytes_written) noexcept {
    switch (kind) {
        case QueryKind::Read: m_reads.fetch_add(1, std::memory_order_relaxed); break;
        case QueryKind::Write: m_writes.fetch_add(1, std::memory_order_relaxed); break;
        default: m_other.fetch_add(1, std::memory_order_relaxed); break;
    }
    if (bytes_read != 0) m_bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
    if (bytes_written != 0) {
        m_bytes_written.fetch_add(bytes_written, std::memory_order_relaxed);
    }
}

void ThreadStats::record_error() noexcept {
    m_errors.fetch_add(1, std::memory_order_relaxed);
}

void ThreadStats::record_reconnect() noexcept {
    m_reconnects.fetch_add(1, std::memory_order_relaxed);
}

StatsSnapshot aggregate(std::span<ThreadStats* const> threads, double percentile) {
    StatsSnapshot result;
    std::array<std::uint64_t, LatencyHistogram::kBins> counts{};
    std::uint64_t timed = 0;
    std::uint64_t min_latency = UINT64_MAX;
    std::uint64_t max_latency = 0;
    std::uint64_t events_min = UINT64_MAX;
    std::uint64_t events_max = 0;
    double events_sum = 0.0;
    double events_squared = 0.0;
    std::uint64_t thread_count = 0;

    for (ThreadStats* thread : threads) {
        if (thread == nullptr) continue;
        ++thread_count;
        result.events += thread->events();
        result.reads += thread->reads();
        result.writes += thread->writes();
        result.other += thread->other();
        result.errors += thread->errors();
        result.reconnects += thread->reconnects();
        result.bytes_read += thread->bytes_read();
        result.bytes_written += thread->bytes_written();
        result.latency_sum_ns += thread->latency_sum_ns();
        max_latency = std::max(max_latency, thread->latency_max_ns());
        min_latency = std::min(min_latency, thread->latency_min_ns());
        if (thread->active()) ++result.threads_running;

        const auto hist = thread->histogram();
        for (std::size_t i = 0; i < counts.size(); ++i) counts[i] += hist.counts[i];
        timed += hist.total;

        const auto thread_events = thread->events();
        events_min = std::min(events_min, thread_events);
        events_max = std::max(events_max, thread_events);
        events_sum += static_cast<double>(thread_events);
        events_squared += static_cast<double>(thread_events) *
                          static_cast<double>(thread_events);
    }

    if (thread_count > 0) {
        result.thread_events_min = events_min == UINT64_MAX ? 0 : events_min;
        result.thread_events_max = events_max;
        result.thread_events_avg = events_sum / static_cast<double>(thread_count);
        const double mean = result.thread_events_avg;
        const double variance =
            std::max(0.0, events_squared / static_cast<double>(thread_count) -
                              mean * mean);
        result.thread_events_stddev = std::sqrt(variance);
        result.threads_running = result.threads_running == 0 ? 0 : result.threads_running;
    }
    result.histogram = LatencyHistogram::Snapshot{counts, timed};
    if (timed > 0) {
        result.latency_avg = static_cast<double>(result.latency_sum_ns) /
                             static_cast<double>(timed) * 1e-9;
        result.latency_min_ns = min_latency;
        result.latency_max_ns = max_latency;
        result.latency_p50 = percentile_from_counts(counts, timed, 0.50);
        result.latency_p95 = percentile_from_counts(counts, timed, 0.95);
        result.latency_p99 = percentile_from_counts(counts, timed, 0.99);
        result.latency_percentile =
            percentile_from_counts(counts, timed, percentile / 100.0);
    }
    return result;
}

StatsSnapshot StatsSnapshot::difference(const StatsSnapshot& older,
                                        double percentile) const {
    StatsSnapshot result;
    result.events = events - older.events;
    result.reads = reads - older.reads;
    result.writes = writes - older.writes;
    result.other = other - older.other;
    result.errors = errors - older.errors;
    result.reconnects = reconnects - older.reconnects;
    result.bytes_read = bytes_read - older.bytes_read;
    result.bytes_written = bytes_written - older.bytes_written;
    result.latency_sum_ns = latency_sum_ns - older.latency_sum_ns;
    result.latency_min_ns = latency_min_ns;
    result.latency_max_ns = latency_max_ns;

    std::array<std::uint64_t, LatencyHistogram::kBins> counts{};
    std::uint64_t timed = 0;
    for (std::size_t i = 0; i < counts.size(); ++i) {
        counts[i] = histogram.counts[i] >= older.histogram.counts[i]
                        ? histogram.counts[i] - older.histogram.counts[i]
                        : 0;
        timed += counts[i];
    }
    result.histogram = LatencyHistogram::Snapshot{counts, timed};
    if (timed > 0) {
        result.latency_avg = static_cast<double>(result.latency_sum_ns) /
                             static_cast<double>(timed) * 1e-9;
        result.latency_p50 = percentile_from_counts(counts, timed, 0.50);
        result.latency_p95 = percentile_from_counts(counts, timed, 0.95);
        result.latency_p99 = percentile_from_counts(counts, timed, 0.99);
        result.latency_percentile =
            percentile_from_counts(counts, timed, percentile / 100.0);
    }
    result.threads_running = threads_running;
    result.thread_events_min = thread_events_min;
    result.thread_events_max = thread_events_max;
    result.thread_events_avg = thread_events_avg;
    result.thread_events_stddev = thread_events_stddev;
    return result;
}

}  // namespace sb
