#include "sysbench/db.hpp"

#include <algorithm>
#include <cstring>
#include <string_view>

namespace sb::db {

Error::Error(int code, std::string_view state, std::string_view message,
             bool fatal) noexcept
    : m_code(code), m_fatal(fatal) {
    if (!state.empty()) {
        const std::size_t count = std::min<std::size_t>(state.size(), 5);
        std::memcpy(m_state, state.data(), count);
        m_state[count] = '\0';
    }
    const std::size_t count = std::min<std::size_t>(message.size(), kMessageCapacity - 1);
    std::memcpy(m_message, message.data(), count);
    m_message[count] = '\0';
}

Error::Error(std::string_view message, int network_code) noexcept
    : m_code(network_code) {
    const std::size_t count = std::min<std::size_t>(message.size(), kMessageCapacity - 1);
    std::memcpy(m_message, message.data(), count);
    m_message[count] = '\0';
}

DriverRegistry& DriverRegistry::instance() {
    static DriverRegistry registry;
    return registry;
}

bool DriverRegistry::add(std::string_view name, Driver::CreateFn factory) noexcept {
    if (name.empty() || factory == nullptr || m_count >= kMaxDrivers) return false;
    for (std::size_t i = 0; i < m_count; ++i) {
        if (m_entries[i].name == name) return false;
    }
    m_entries[m_count++] = Entry{name, factory};
    return true;
}

Driver* DriverRegistry::create(std::string_view name,
                               mem::MemoryResource& resource) const {
    for (std::size_t i = 0; i < m_count; ++i) {
        if (m_entries[i].name == name) return m_entries[i].create(resource);
    }
    return nullptr;
}

bool DriverRegistry::contains(std::string_view name) const noexcept {
    for (std::size_t i = 0; i < m_count; ++i) {
        if (m_entries[i].name == name) return true;
    }
    return false;
}

std::string_view DriverRegistry::name_at(std::size_t index) const noexcept {
    if (index >= m_count) return {};
    return m_entries[index].name;
}

}  // namespace sb::db
