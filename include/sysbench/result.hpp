#pragma once

// Rust-style fallible result used by every blocking interface that can fail.
// Control flow stays explicit at call sites:
//
//   auto result = co_await connection->query(sql, arena);
//   if (result.is_err()) co_return Result<T, db::Error>::err(result.error());
//   auto& rows = result.value().rows;
//
// Values live in place; constructing a Result never allocates.

#include <optional>
#include <type_traits>
#include <utility>

namespace sb {

template <class T, class E>
class [[nodiscard]] Result {
public:
    static_assert(!std::is_void_v<T>);
    static_assert(!std::is_void_v<E>);

    static Result ok(T value) { return Result(std::move(value)); }
    static Result err(E error) { return Result(std::move(error), ErrorTag{}); }

    [[nodiscard]] bool is_ok() const noexcept { return m_value.has_value(); }
    [[nodiscard]] bool is_err() const noexcept { return m_error.has_value(); }
    explicit operator bool() const noexcept { return is_ok(); }

    T& value() & { return *m_value; }
    const T& value() const& { return *m_value; }
    T&& value() && { return std::move(*m_value); }

    E& error() & { return *m_error; }
    const E& error() const& { return *m_error; }
    E&& error() && { return std::move(*m_error); }

private:
    struct ErrorTag {};
    Result(T value) : m_value(std::move(value)) {}
    Result(E error, ErrorTag) : m_error(std::move(error)) {}

    std::optional<T> m_value;
    std::optional<E> m_error;
};

template <class E>
class [[nodiscard]] Result<void, E> {
public:
    static Result ok() { return Result(); }
    static Result err(E error) { return Result(std::move(error)); }

    [[nodiscard]] bool is_ok() const noexcept { return !m_error.has_value(); }
    [[nodiscard]] bool is_err() const noexcept { return m_error.has_value(); }
    explicit operator bool() const noexcept { return is_ok(); }

    E& error() & { return *m_error; }
    const E& error() const& { return *m_error; }
    E&& error() && { return std::move(*m_error); }

private:
    Result() = default;
    explicit Result(E error) : m_error(std::move(error)) {}

    std::optional<E> m_error;
};

}  // namespace sb
