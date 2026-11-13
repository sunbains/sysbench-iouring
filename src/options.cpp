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

#include "sysbench/options.hpp"

#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <array>
#include <string_view>
#include <cstring>

namespace sb {
namespace {

std::string_view strip_dashes(std::string_view value) {
    if (value.starts_with("--")) return value.substr(2);
    if (value.starts_with("-")) return value.substr(1);
    return value;
}

}  // namespace

OptionDef::OptionDef(mem::MemoryResource& resource, std::string_view name_value,
                     std::string_view description_value, OptionType type_value,
                     std::string_view default_value, bool custom_value)
    : m_name(mem::Allocator<char>(&resource)),
      m_description(mem::Allocator<char>(&resource)),
      m_value(mem::Allocator<char>(&resource)),
      m_type(type_value),
      m_custom(custom_value) {
    m_name.assign(name_value);
    m_description.assign(description_value);
    m_value.assign(default_value);
}

Options::Options(mem::MemoryResource& resource) : m_resource(&resource) {
    add("threads", "Number of benchmark threads", OptionType::Int, "1");
    add("time", "Limit for total execution time in seconds", OptionType::Int, "10");
    add("events", "Limit for total number of events", OptionType::Int, "0");
    add("rate", "Average transactions rate. 0 for unlimited", OptionType::Int, "0");
    add("report-interval", "Report intermediate statistics every N seconds",
        OptionType::Int, "1");
    add("percentile", "Percentile to report for latency", OptionType::Int, "95");
    add("histogram", "Print latency histogram", OptionType::Bool, "off");
    add("warmup-time", "Warmup time in seconds", OptionType::Int, "0");
    add("db-driver", "Database driver to use", OptionType::String, "mysql");
    add("mysql-host", "MySQL host", OptionType::String, "127.0.0.1");
    add("mysql-port", "MySQL port", OptionType::String, "3306");
    add("mysql-user", "MySQL user", OptionType::String, "root");
    add("mysql-password", "MySQL password", OptionType::String, "");
    add("mysql-db", "MySQL database", OptionType::String, "");
    add("mysql-socket", "MySQL Unix socket path", OptionType::String, "");
    add("mysql-ignore-errors", "MySQL error codes to ignore", OptionType::List,
        "1213,1205");
    add("mysql-ssl", "MySQL TLS mode: off, preferred, required",
        OptionType::String, "off");
    add("mysql-ssl-verify-server-cert", "Verify the MySQL TLS server certificate",
        OptionType::Bool, "off");
    add("mysql-ssl-ca", "CA file for MySQL TLS verification", OptionType::String,
        "");
    add("mysql-ssl-cert", "Client certificate for MySQL TLS", OptionType::String,
        "");
    add("mysql-ssl-key", "Client private key for MySQL TLS", OptionType::String,
        "");
    add("memory-size", "Up-front global allocator reservation", OptionType::Size,
        "4G");
    add("populate-memory", "Fault in the global allocation up front",
        OptionType::Bool, "off");
    add("workers", "Runtime worker threads (max 64; 0 = hardware concurrency)",
        OptionType::Int, "0");
    add("numa", "Enable NUMA-aware placement and node-local memory",
        OptionType::Bool, "on");
    add("pin-workers", "Pin worker threads to their assigned CPUs",
        OptionType::Bool, "on");
    add("stall-timeout",
        "Report a stalled connection after N seconds without progress "
        "(0 = wait forever)",
        OptionType::Int, "60");
    add("seed", "Random seed (0 = time based)", OptionType::Int, "0");
    add("rand-seed", "Alias for --seed", OptionType::Int, "0");
    add("rand-pareto-h", "Shape parameter for Pareto distribution",
        OptionType::Double, "0.2");
    add("rand-zipfian-exp", "Exponent for Zipfian distribution",
        OptionType::Double, "0.0");
    add("verbose", "Verbosity level", OptionType::Int, "0");
    add("debug", "Enable debug logging", OptionType::Bool, "off");
    add("rand-type", "Random distribution: uniform, gaussian, pareto, zipfian",
        OptionType::String, "uniform");
}

Options::~Options() {
    for (std::size_t i = 0; i < m_count; ++i) {
        m_entries[i]->~OptionDef();
        m_resource->deallocate(m_entries[i], sizeof(OptionDef), alignof(OptionDef));
    }
}

mem::String Options::make_error(mem::MemoryResource& resource,
                                std::string_view text) {
    mem::String error{mem::Allocator<char>(&resource)};
    error.assign(text);
    return error;
}

bool Options::add(std::string_view name, std::string_view description,
                  OptionType type, std::string_view default_value, bool custom) {
    name = strip_dashes(name);
    if (name.empty() || m_count >= kMaxOptions) return false;
    if (find(name) != nullptr) return true;

    void* memory = m_resource->allocate(sizeof(OptionDef), alignof(OptionDef));
    auto* option = ::new (memory) OptionDef(*m_resource, name, description, type,
                                            default_value, custom);
    m_entries[m_count++] = option;
    return true;
}

OptionDef* Options::find(std::string_view name) noexcept {
    name = strip_dashes(name);
    for (std::size_t i = 0; i < m_count; ++i) {
        if (m_entries[i]->m_name == name) return m_entries[i];
    }
    char normalized[192]{};
    if (name.size() < sizeof(normalized)) {
        for (std::size_t i = 0; i < name.size(); ++i) {
            normalized[i] = name[i] == '-' ? '_' : name[i];
        }
        const std::string_view alternate(normalized, name.size());
        for (std::size_t i = 0; i < m_count; ++i) {
            if (m_entries[i]->m_name == alternate) return m_entries[i];
        }
        for (std::size_t i = 0; i < name.size(); ++i) {
            normalized[i] = name[i] == '_' ? '-' : name[i];
        }
        for (std::size_t i = 0; i < m_count; ++i) {
            if (m_entries[i]->m_name == std::string_view(normalized, name.size())) {
                return m_entries[i];
            }
        }
    }
    return nullptr;
}

const OptionDef* Options::find(std::string_view name) const noexcept {
    name = strip_dashes(name);
    for (std::size_t i = 0; i < m_count; ++i) {
        if (m_entries[i]->m_name == name) return m_entries[i];
    }
    char normalized[192]{};
    if (name.size() < sizeof(normalized)) {
        for (std::size_t i = 0; i < name.size(); ++i) {
            normalized[i] = name[i] == '-' ? '_' : name[i];
        }
        const std::string_view alternate(normalized, name.size());
        for (std::size_t i = 0; i < m_count; ++i) {
            if (m_entries[i]->m_name == alternate) return m_entries[i];
        }
    }
    return nullptr;
}

OptionDef* Options::at(std::size_t index) noexcept {
    return index < m_count ? m_entries[index] : nullptr;
}

const OptionDef* Options::at(std::size_t index) const noexcept {
    return index < m_count ? m_entries[index] : nullptr;
}

bool Options::parse_bool_value(std::string_view value, bool& result) noexcept {
    char buffer[16]{};
    const std::size_t length = std::min(value.size(), sizeof(buffer) - 1);
    for (std::size_t i = 0; i < length; ++i) {
        buffer[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(value[i])));
    }
    const std::string_view lowered(buffer, length);
    if (lowered.empty() || lowered == "on" || lowered == "yes" || lowered == "true" ||
        lowered == "1") {
        result = true;
        return true;
    }
    if (lowered == "off" || lowered == "no" || lowered == "false" || lowered == "0") {
        result = false;
        return true;
    }
    return false;
}

bool Options::parse_int_value(std::string_view value,
                              std::int64_t& result) noexcept {
    if (value.empty()) return false;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto parsed = std::from_chars(begin, end, result);
    return parsed.ec == std::errc{} && parsed.ptr == end;
}

bool Options::parse_size_value(std::string_view value,
                               std::uint64_t& result) noexcept {
    if (value.empty()) return false;
    std::uint64_t multiplier = 1;
    const char last = static_cast<char>(
        std::tolower(static_cast<unsigned char>(value.back())));
    if (last == 'k' || last == 'm' || last == 'g' || last == 't') {
        multiplier = last == 'k'   ? 1024ull
                     : last == 'm' ? 1024ull * 1024
                     : last == 'g' ? 1024ull * 1024 * 1024
                                   : 1024ull * 1024 * 1024 * 1024;
        value.remove_suffix(1);
        if (!value.empty() && (value.back() == 'b' || value.back() == 'B')) {
            value.remove_suffix(1);
        }
    } else if (last == 'b') {
        value.remove_suffix(1);
    }

    double numeric = 0.0;
    if (!parse_double_value(value, numeric) || numeric < 0.0) return false;
    const double scaled = numeric * static_cast<double>(multiplier);
    if (scaled > static_cast<double>(UINT64_MAX)) return false;
    result = static_cast<std::uint64_t>(scaled);
    return true;
}

bool Options::parse_double_value(std::string_view value, double& result) noexcept {
    if (value.empty()) return false;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto parsed = std::from_chars(begin, end, result);
    return parsed.ec == std::errc{} && parsed.ptr == end && std::isfinite(result);
}

bool Options::set(std::string_view name, std::string_view value,
                  mem::String& error) {
    OptionDef* option = find(name);
    if (option == nullptr) {
        error = make_error(*m_resource, "unknown option '--");
        error.append(name);
        error.push_back('\'');
        return false;
    }

    switch (option->m_type) {
        case OptionType::Bool: {
            bool parsed = false;
            if (!value.empty() && !parse_bool_value(value, parsed)) {
                error = make_error(*m_resource, "invalid boolean value for --");
                error.append(option->m_name);
                return false;
            }
            if (value.empty()) parsed = true;
            option->m_value.assign(parsed ? "on" : "off");
            return true;
        }
        case OptionType::Int: {
            std::int64_t parsed = 0;
            if (!parse_int_value(value, parsed)) {
                error = make_error(*m_resource, "invalid integer value for --");
                error.append(option->m_name);
                return false;
            }
            char buffer[32]{};
            const auto result = std::to_chars(buffer, buffer + sizeof(buffer), parsed);
            option->m_value.assign(buffer, result.ptr);
            return true;
        }
        case OptionType::Size: {
            std::uint64_t parsed = 0;
            if (!parse_size_value(value, parsed)) {
                error = make_error(*m_resource, "invalid size value for --");
                error.append(option->m_name);
                return false;
            }
            char buffer[32]{};
            const auto result = std::to_chars(buffer, buffer + sizeof(buffer), parsed);
            option->m_value.assign(buffer, result.ptr);
            return true;
        }
        case OptionType::Double: {
            double parsed = 0.0;
            if (!parse_double_value(value, parsed)) {
                error = make_error(*m_resource, "invalid number for --");
                error.append(option->m_name);
                return false;
            }
            char buffer[64]{};
            const auto result = std::to_chars(buffer, buffer + sizeof(buffer), parsed);
            option->m_value.assign(buffer, result.ptr);
            return true;
        }
        case OptionType::String:
        case OptionType::List:
            option->m_value.assign(value);
            return true;
    }
    return false;
}

bool Options::as_bool(std::string_view name, bool fallback) const {
    const OptionDef* option = find(name);
    if (option == nullptr) return fallback;
    bool result = fallback;
    return parse_bool_value(option->m_value, result) ? result : fallback;
}

std::int64_t Options::as_int(std::string_view name, std::int64_t fallback) const {
    const OptionDef* option = find(name);
    if (option == nullptr) return fallback;
    std::int64_t result = fallback;
    return parse_int_value(option->m_value, result) ? result : fallback;
}

std::uint64_t Options::as_size(std::string_view name,
                               std::uint64_t fallback) const {
    const OptionDef* option = find(name);
    if (option == nullptr) return fallback;
    std::uint64_t result = fallback;
    return parse_size_value(option->m_value, result) ? result : fallback;
}

double Options::as_double(std::string_view name, double fallback) const {
    const OptionDef* option = find(name);
    if (option == nullptr) return fallback;
    double result = fallback;
    return parse_double_value(option->m_value, result) ? result : fallback;
}

std::string_view Options::as_string(std::string_view name,
                                    std::string_view fallback) const {
    const OptionDef* option = find(name);
    return option != nullptr ? std::string_view(option->m_value) : fallback;
}

bool Options::looks_like_script(std::string_view token) const {
    if (token.empty() || token.starts_with("-")) return false;
    if (token.ends_with(".lua") || token.find('/') != std::string_view::npos) {
        return true;
    }
    std::array<char, 4096> path{};
    if (token.size() < path.size()) {
        std::memcpy(path.data(), token.data(), token.size());
        if (::access(path.data(), R_OK) == 0) return true;
    }
    constexpr std::string_view directory = SYSBENCH_LUA_DIR "/";
    if (directory.size() + token.size() + 4 < path.size()) {
        std::memcpy(path.data(), directory.data(), directory.size());
        std::memcpy(path.data() + directory.size(), token.data(), token.size());
        std::memcpy(path.data() + directory.size() + token.size(), ".lua", 4);
        if (::access(path.data(), R_OK) == 0) return true;
    }
    return false;
}

bool Options::is_command(std::string_view token) const {
    return token == "run" || token == "prepare" || token == "cleanup" ||
           token == "help" || token == "warmup" || token == "prewarm";
}

mem::String Options::resolve_script(std::string_view token,
                                    mem::String& error) const {
    mem::String result{mem::Allocator<char>(m_resource)};
    std::array<char, 4096> direct{};
    if (token.size() < direct.size()) {
        std::memcpy(direct.data(), token.data(), token.size());
        if (::access(direct.data(), R_OK) == 0) {
            result.assign(token);
            return result;
        }
    }
    mem::String candidate{mem::Allocator<char>(m_resource)};
    candidate.assign(SYSBENCH_LUA_DIR "/");
    candidate.append(token);
    if (!token.ends_with(".lua")) candidate.append(".lua");
    if (::access(candidate.c_str(), R_OK) == 0) {
        result.assign(candidate);
        return result;
    }
    error = make_error(*m_resource, "cannot find benchmark script '");
    error.append(token);
    error.push_back('\'');
    return result;
}

bool Options::scan_arguments(int argc, char** argv, ParsedArgs& parsed,
                             mem::String& error) {
    parsed = ParsedArgs{mem::String{mem::Allocator<char>(m_resource)},
                        mem::String{mem::Allocator<char>(m_resource)},
                        mem::String{mem::Allocator<char>(m_resource)}};
    const int kMaxCandidates = 32;
    std::string_view candidates[kMaxCandidates]{};
    int candidate_indices[kMaxCandidates]{};
    int candidate_count = 0;
    bool after_separator = false;

    for (int i = 1; i < argc; ++i) {
        const std::string_view token(argv[i]);
        if (!after_separator && token == "--") {
            after_separator = true;
            continue;
        }
        if (!after_separator && (token == "--help" || token == "-h")) {
            parsed.m_help = true;
            continue;
        }
        if (!after_separator && (token == "--version" || token == "-V")) {
            parsed.m_version = true;
            continue;
        }
        if (!after_separator && token.starts_with("--")) {
            const auto equals = token.find('=');
            const std::string_view name =
                equals == std::string_view::npos ? token.substr(2)
                                                 : token.substr(2, equals - 2);
            const OptionDef* option = find(name);
            if (equals == std::string_view::npos && option != nullptr &&
                option->m_type != OptionType::Bool && i + 1 < argc) {
                ++i;
            }
            continue;
        }
        if (!after_separator && token.starts_with("-") && token.size() > 1) {
            continue;
        }
        if (candidate_count < kMaxCandidates) {
            candidates[candidate_count] = token;
            candidate_indices[candidate_count] = i;
            ++candidate_count;
        }
    }

    if (candidate_count == 0) {
        if (parsed.m_help || parsed.m_version) {
            parsed.m_valid = true;
            return true;
        }
        error = make_error(*m_resource, "no benchmark script specified");
        return false;
    }

    int script_candidate = -1;
    for (int i = 0; i < candidate_count; ++i) {
        if (looks_like_script(candidates[i])) {
            script_candidate = i;
            break;
        }
    }
    if (script_candidate < 0) script_candidate = 0;

    parsed.m_script_arg.assign(candidates[script_candidate]);
    parsed.m_script_index = candidate_indices[script_candidate];
    parsed.m_script_path = resolve_script(parsed.m_script_arg, error);
    if (parsed.m_script_path.empty()) return false;

    int command_candidate = -1;
    for (int i = script_candidate + 1; i < candidate_count; ++i) {
        if (is_command(candidates[i])) command_candidate = i;
    }
    if (command_candidate < 0 && script_candidate + 1 < candidate_count) {
        command_candidate = script_candidate + 1;
    }
    if (command_candidate >= 0) {
        parsed.m_command.assign(candidates[command_candidate]);
        parsed.m_command_index = candidate_indices[command_candidate];
    }
    if (parsed.m_command.empty() && !parsed.m_help && !parsed.m_version) {
        error = make_error(*m_resource, "no command specified (run, prepare, cleanup, help)");
        return false;
    }
    parsed.m_valid = true;
    return true;
}

bool Options::apply_arguments(int argc, char** argv, const ParsedArgs& parsed,
                              mem::String& error) {
    for (int i = 1; i < argc; ++i) {
        if (i == parsed.m_script_index || i == parsed.m_command_index) continue;
        const std::string_view token(argv[i]);
        if (token == "--" || token == "--help" || token == "-h" ||
            token == "--version" || token == "-V") {
            continue;
        }
        if (!token.starts_with("--")) {
            if (token.starts_with('-') && token.size() > 1) {
                error = make_error(*m_resource, "unknown option '");
                error.append(token);
                error.push_back('\'');
                return false;
            }
            if (i > 0 && i - 1 != parsed.m_script_index && i - 1 != parsed.m_command_index) {
                // Extra positional tokens are accepted only when they were
                // consumed as values by an earlier option.
            }
            continue;
        }

        std::string_view name = token.substr(2);
        std::string_view value;
        const auto equals = name.find('=');
        if (equals != std::string_view::npos) {
            value = name.substr(equals + 1);
            name = name.substr(0, equals);
            if (!set(name, value, error)) return false;
            continue;
        }

        const OptionDef* option = find(name);
        if (option == nullptr) {
            error = make_error(*m_resource, "unknown option '--");
            error.append(name);
            error.push_back('\'');
            return false;
        }
        if (option->m_type == OptionType::Bool) {
            if (i + 1 < argc) {
                bool parsed_bool = false;
                if (parse_bool_value(argv[i + 1], parsed_bool)) {
                    value = argv[++i];
                } else {
                    value = "on";
                }
            } else {
                value = "on";
            }
            if (!set(name, value, error)) return false;
            continue;
        }
        if (i + 1 >= argc) {
            error = make_error(*m_resource, "missing value for --");
            error.append(name);
            return false;
        }
        value = argv[++i];
        if (!set(name, value, error)) return false;
    }
    return true;
}

}  // namespace sb
