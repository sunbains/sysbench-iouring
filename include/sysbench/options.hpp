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

#include "sysbench/memory.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace sb {

enum class OptionType : std::uint8_t { Bool, Int, Size, Double, String, List };

struct OptionDef {
    mem::String m_name;
    mem::String m_description;
    mem::String m_value;
    OptionType m_type = OptionType::String;
    bool m_custom = false;

    OptionDef(mem::MemoryResource& resource, std::string_view name,
              std::string_view description, OptionType type,
              std::string_view value, bool custom);
    OptionDef(const OptionDef&) = delete;
    OptionDef& operator=(const OptionDef&) = delete;
    OptionDef(OptionDef&&) = delete;
    OptionDef& operator=(OptionDef&&) = delete;
};

struct ParsedArgs {
    mem::String m_script_arg;
    mem::String m_script_path;
    mem::String m_command;
    int m_script_index = -1;
    int m_command_index = -1;
    bool m_help = false;
    bool m_version = false;
    bool m_valid = false;
};

class Options {
public:
    static constexpr std::size_t kMaxOptions = 192;

    explicit Options(mem::MemoryResource& resource);
    ~Options();

    Options(const Options&) = delete;
    Options& operator=(const Options&) = delete;

    bool add(std::string_view name, std::string_view description, OptionType type,
             std::string_view default_value, bool custom = false);
    [[nodiscard]] OptionDef* find(std::string_view name) noexcept;
    [[nodiscard]] const OptionDef* find(std::string_view name) const noexcept;
    [[nodiscard]] OptionDef* at(std::size_t index) noexcept;
    [[nodiscard]] const OptionDef* at(std::size_t index) const noexcept;
    [[nodiscard]] std::size_t size() const noexcept { return m_count; }

    bool set(std::string_view name, std::string_view value, mem::String& error);

    [[nodiscard]] bool as_bool(std::string_view name, bool fallback = false) const;
    [[nodiscard]] std::int64_t as_int(std::string_view name,
                                      std::int64_t fallback = 0) const;
    [[nodiscard]] std::uint64_t as_size(std::string_view name,
                                        std::uint64_t fallback = 0) const;
    [[nodiscard]] double as_double(std::string_view name,
                                   double fallback = 0.0) const;
    [[nodiscard]] std::string_view as_string(std::string_view name,
                                             std::string_view fallback = {}) const;

    // First pass: identify the script and command without requiring custom
    // option definitions to have been loaded yet.
    bool scan_arguments(int argc, char** argv, ParsedArgs& parsed,
                        mem::String& error);
    // Second pass: apply every --option with the complete definition table.
    bool apply_arguments(int argc, char** argv, const ParsedArgs& parsed,
                         mem::String& error);

    static bool parse_bool_value(std::string_view value, bool& result) noexcept;
    static bool parse_int_value(std::string_view value, std::int64_t& result) noexcept;
    static bool parse_size_value(std::string_view value, std::uint64_t& result) noexcept;
    static bool parse_double_value(std::string_view value, double& result) noexcept;

private:
    static mem::String make_error(mem::MemoryResource& resource,
                                  std::string_view text);
    bool looks_like_script(std::string_view token) const;
    bool is_command(std::string_view token) const;
    [[nodiscard]] mem::String resolve_script(std::string_view token,
                                             mem::String& error) const;

    mem::MemoryResource* m_resource;
    OptionDef* m_entries[kMaxOptions]{};
    std::size_t m_count = 0;
};

}  // namespace sb
