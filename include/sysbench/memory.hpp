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

// Hierarchical, pre-reserved memory subsystem.
//
// Ownership is strictly top-down:
//
//   GlobalAllocator (one mmap, reserved up front)
//     -> HeapArena per runtime worker (long-lived objects)
//          -> BumpArena per benchmark/coroutine (event lifetime)
//               -> ScopedAllocator (nested, LIFO, per-event scope)
//
// No subsystem in the project calls operator new/malloc/mmap directly.  The
// only OS allocation is the root mapping.  Short-lived allocations reset in
// O(1); long-lived arenas use boundary-tag coalescing so Lua, connections and
// prepared statements can allocate and free in arbitrary order.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace sb::mem {

inline constexpr std::size_t kCacheLine = 64;
inline constexpr std::size_t kDefaultAlignment = alignof(std::max_align_t);

struct ResourceStats {
    std::size_t m_capacity = 0;
    std::size_t m_used = 0;
    std::size_t m_peak = 0;
    std::size_t m_allocations = 0;
    std::size_t m_frees = 0;
    std::size_t m_failed = 0;
};

class MemoryResource {
public:
    virtual ~MemoryResource() = default;

    virtual void* allocate(std::size_t bytes,
                           std::size_t alignment = kDefaultAlignment) = 0;
    virtual void deallocate(void* ptr, std::size_t bytes,
                            std::size_t alignment = kDefaultAlignment) noexcept = 0;
    virtual bool owns(const void* ptr) const noexcept = 0;
    [[nodiscard]] virtual ResourceStats stats() const noexcept = 0;
    [[nodiscard]] virtual std::string_view name() const noexcept = 0;
};

// One large MAP_NORESERVE mapping carved with a single atomic CAS.  The global
// allocator never returns memory to the OS; its children own their regions for
// their entire lifetime.  map_populate=true makes the reservation physically
// resident up front (useful for latency benchmarks).
class RootArena final : public MemoryResource {
public:
    RootArena(std::size_t capacity, bool map_populate = false);
    ~RootArena() override;

    RootArena(const RootArena&) = delete;
    RootArena& operator=(const RootArena&) = delete;

    void* allocate(std::size_t bytes,
                   std::size_t alignment = kDefaultAlignment) override;
    void deallocate(void*, std::size_t, std::size_t) noexcept override {}
    bool owns(const void* ptr) const noexcept override;
    [[nodiscard]] ResourceStats stats() const noexcept override;
    [[nodiscard]] std::string_view name() const noexcept override { return "root"; }

    [[nodiscard]] void* base() const noexcept { return m_base; }
    [[nodiscard]] std::size_t capacity() const noexcept { return m_capacity; }
    [[nodiscard]] bool populated() const noexcept { return m_populated; }

private:
    void* m_base = nullptr;
    std::size_t m_capacity = 0;
    bool m_populated = false;
    alignas(kCacheLine) std::atomic<std::size_t> m_offset{0};
    alignas(kCacheLine) std::atomic<std::size_t> m_peak{0};
    std::atomic<std::size_t> m_allocations{0};
    std::atomic<std::size_t> m_failures{0};
};

// General-purpose suballocator over a contiguous region obtained from a parent.
// Blocks carry boundary tags and are kept in segregated free lists.  The arena
// is intentionally single-threaded: each runtime worker owns one instance.
class HeapArena final : public MemoryResource {
public:
    static constexpr std::size_t kMinBlock = 64;
    static constexpr std::size_t kMaxAlignment = 4096;
    /* Free lists (two-level segregated fit): blocks below 1 KiB by exact
    size (16-byte classes), larger ones in 8 bins per power of two.  A bitmap
    of non-empty bins makes a search a few bit operations: no list walk. */
    static constexpr std::size_t kSmallLimit = 1024;
    static constexpr std::size_t kSmallBins = kSmallLimit / 16;
    static constexpr unsigned kSubBinBits = 3;
    static constexpr std::size_t kBinCount = kSmallBins + (32 - 10) * (1u << kSubBinBits);

    HeapArena(MemoryResource& parent, std::size_t bytes,
              std::string_view name = "heap", int numa_node = -1);
    ~HeapArena() override;

    HeapArena(const HeapArena&) = delete;
    HeapArena& operator=(const HeapArena&) = delete;

    void* allocate(std::size_t bytes,
                   std::size_t alignment = kDefaultAlignment) override;
    void deallocate(void* ptr, std::size_t bytes,
                    std::size_t alignment = kDefaultAlignment) noexcept override;
    bool owns(const void* ptr) const noexcept override;
    [[nodiscard]] ResourceStats stats() const noexcept override;
    [[nodiscard]] std::string_view name() const noexcept override { return m_name; }

    [[nodiscard]] void* base() const noexcept { return m_base; }
    [[nodiscard]] int numa_node() const noexcept { return m_numa_node; }
    [[nodiscard]] bool numa_bound() const noexcept { return m_numa_bound; }

    // Escape hatch for callers that know an arena is quiescent.
    void reset() noexcept;

private:
    struct alignas(16) Block {
        std::uint32_t m_size = 0;       // total block size, including this header
        std::uint32_t m_prev_size = 0;  // total size of previous block, 0 if first
        std::uint32_t m_is_free = 0;
        std::uint32_t m_bin = 0;
        Block* m_next_free = nullptr;
        Block* m_prev_free = nullptr;
    };

    static constexpr std::uint32_t kUsedBin = std::numeric_limits<std::uint32_t>::max();

    [[nodiscard]] static std::size_t round_up(std::size_t value,
                                              std::size_t alignment) noexcept {
        return (value + alignment - 1) & ~(alignment - 1);
    }
    [[nodiscard]] static std::uint32_t bin_for(std::size_t bytes) noexcept;
    // The first bin whose every block holds `bytes`.
    [[nodiscard]] static std::uint32_t search_bin(std::size_t bytes) noexcept;
    [[nodiscard]] Block* block_of(void* ptr) const noexcept;
    [[nodiscard]] void* user_of(Block* block) const noexcept;
    void insert_free(Block* block) noexcept;
    void remove_free(Block* block) noexcept;
    void split(Block* block, std::size_t wanted) noexcept;
    void coalesce(Block* block) noexcept;
    [[nodiscard]] Block* find_fit(std::size_t wanted) noexcept;

    MemoryResource* m_parent = nullptr;
    std::byte* m_base = nullptr;
    std::size_t m_capacity = 0;
    std::size_t m_used = 0;
    std::size_t m_peak = 0;
    std::size_t m_allocations = 0;
    std::size_t m_frees = 0;
    std::size_t m_failures = 0;
    /* Bin heads as 32-bit block indices (offset / 16 + 1; 0: empty), half
    the size of pointers: heaps are embedded in small objects. */
    [[nodiscard]] Block* head(std::size_t bin) const noexcept {
        const std::uint32_t index = m_bins[bin];
        return index == 0 ? nullptr
                          : reinterpret_cast<Block*>(m_base + (std::size_t{index - 1} << 4));
    }
    void set_head(std::size_t bin, Block* block) noexcept {
        m_bins[bin] = block == nullptr
                          ? 0
                          : static_cast<std::uint32_t>(
                                ((reinterpret_cast<std::byte*>(block) - m_base) >> 4) + 1);
    }
    std::uint32_t m_bins[kBinCount] = {};
    std::uint64_t m_bin_map[(kBinCount + 63) / 64] = {};
    std::string_view m_name;
    int m_numa_node = -1;
    bool m_numa_bound = false;
};

// Append-only arena with a growable chain of blocks.  A task owns one BumpArena;
// event scopes mark and rewind it, releasing whole blocks back to the worker heap.
// deallocate() is a no-op, which is what makes event setup effectively free.
class BumpArena final : public MemoryResource {
public:
    struct Mark {
        void* m_block = nullptr;
        std::size_t m_used = 0;
        std::size_t m_generation = 0;
    };

    BumpArena(MemoryResource& parent, std::size_t block_size = 64 * 1024,
              std::string_view name = "bump");
    ~BumpArena() override;

    BumpArena(const BumpArena&) = delete;
    BumpArena& operator=(const BumpArena&) = delete;

    void* allocate(std::size_t bytes,
                   std::size_t alignment = kDefaultAlignment) override;
    void deallocate(void*, std::size_t, std::size_t) noexcept override {}
    bool owns(const void* ptr) const noexcept override;
    [[nodiscard]] ResourceStats stats() const noexcept override;
    [[nodiscard]] std::string_view name() const noexcept override { return m_name; }

    [[nodiscard]] Mark mark() const noexcept;
    void rewind(Mark mark) noexcept;

private:
    struct alignas(16) Block {
        Block* m_next = nullptr;
        std::size_t m_capacity = 0;
        std::size_t m_used = 0;
    };

    static constexpr std::size_t kHeader = sizeof(Block);
    static constexpr std::size_t kHeaderAligned = (kHeader + 15) & ~std::size_t(15);

    [[nodiscard]] Block* new_block(std::size_t payload);
    [[nodiscard]] static std::byte* payload_of(Block* block) noexcept {
        return reinterpret_cast<std::byte*>(block) + kHeaderAligned;
    }

    MemoryResource* m_parent = nullptr;
    std::size_t m_block_size = 0;
    Block* m_head = nullptr;
    Block* m_current = nullptr;
    std::size_t m_used = 0;
    std::size_t m_peak = 0;
    std::size_t m_allocations = 0;
    std::size_t m_failures = 0;
    std::size_t m_total_capacity = 0;
    std::uint64_t m_generation = 0;
    std::string_view m_name;
};

// Strict LIFO scoped allocator.  Construct it at the beginning of an event and
// let its destructor rewind the owning BumpArena.  Nested scopes are supported.
class ScopedAllocator final : public MemoryResource {
public:
    explicit ScopedAllocator(BumpArena& arena) noexcept
        : m_arena(&arena), m_mark(arena.mark()) {}
    ~ScopedAllocator() override { m_arena->rewind(m_mark); }

    ScopedAllocator(const ScopedAllocator&) = delete;
    ScopedAllocator& operator=(const ScopedAllocator&) = delete;

    // Release everything allocated since construction/the previous reset.
    void reset() noexcept {
        m_arena->rewind(m_mark);
        m_mark = m_arena->mark();
    }

    void* allocate(std::size_t bytes,
                   std::size_t alignment = kDefaultAlignment) override {
        return m_arena->allocate(bytes, alignment);
    }
    void deallocate(void*, std::size_t, std::size_t) noexcept override {}
    bool owns(const void* ptr) const noexcept override { return m_arena->owns(ptr); }
    [[nodiscard]] ResourceStats stats() const noexcept override {
        return m_arena->stats();
    }
    [[nodiscard]] std::string_view name() const noexcept override { return "scope"; }

private:
    BumpArena* m_arena;
    BumpArena::Mark m_mark;
};

// Process-wide root allocator.  initialize() must run before any other module.
class GlobalAllocator {
public:
    static GlobalAllocator& instance();

    void initialize(std::size_t capacity, bool map_populate = false);
    void initialize_if_needed(std::size_t capacity = std::size_t{1} << 30,
                              bool map_populate = false);
    [[nodiscard]] bool initialized() const noexcept { return m_root != nullptr; }
    [[nodiscard]] RootArena& root();
    [[nodiscard]] const RootArena& root() const;

private:
    GlobalAllocator() = default;
    GlobalAllocator(const GlobalAllocator&) = delete;
    GlobalAllocator& operator=(const GlobalAllocator&) = delete;

    alignas(RootArena) std::byte m_storage[sizeof(RootArena)]{};
    RootArena* m_root = nullptr;
};

[[nodiscard]] inline RootArena& global_root() {
    return GlobalAllocator::instance().root();
}

// Coroutine frame pool.  Frames are reused through size-class free lists so a
// benchmark that awaits I/O millions of times does not grow the root arena.
void* allocate_task_frame(std::size_t size);
void deallocate_task_frame(void* pointer, std::size_t size) noexcept;
// Select the per-node frame pool used by the calling thread.  Runtime workers
// set this to their NUMA node; the main thread sets its home node.
void set_thread_frame_node(int node) noexcept;
[[nodiscard]] int thread_frame_node() noexcept;

// STL-compatible allocator adapter.  Containers embedded in the hot path should
// use this (or the Vector/String aliases) so their storage is accounted for by
// the owning scoped arena.
template <class T>
class Allocator {
public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    using is_always_equal = std::false_type;

    Allocator() noexcept : m_resource(&global_root()) {}
    explicit Allocator(MemoryResource* resource) noexcept : m_resource(resource) {}
    template <class U>
    Allocator(const Allocator<U>& other) noexcept : m_resource(other.resource()) {}

    [[nodiscard]] T* allocate(std::size_t n) {
        if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        return static_cast<T*>(m_resource->allocate(n * sizeof(T), alignof(T)));
    }

    void deallocate(T* ptr, std::size_t n) noexcept {
        m_resource->deallocate(ptr, n * sizeof(T), alignof(T));
    }

    [[nodiscard]] MemoryResource* resource() const noexcept { return m_resource; }

    template <class U>
    bool operator==(const Allocator<U>& other) const noexcept {
        return m_resource == other.resource();
    }
    template <class U>
    bool operator!=(const Allocator<U>& other) const noexcept {
        return !(*this == other);
    }

private:
    MemoryResource* m_resource;
};

template <class T>
using Vector = std::vector<T, Allocator<T>>;

using String = std::basic_string<char, std::char_traits<char>, Allocator<char>>;

template <class T, class... Args>
[[nodiscard]] T* construct(MemoryResource& resource, Args&&... args) {
    void* memory = resource.allocate(sizeof(T), alignof(T));
    try {
        return ::new (memory) T(std::forward<Args>(args)...);
    } catch (...) {
        resource.deallocate(memory, sizeof(T), alignof(T));
        throw;
    }
}

template <class T>
void destroy(MemoryResource& resource, T* object) noexcept {
    if (object == nullptr) return;
    object->~T();
    resource.deallocate(object, sizeof(T), alignof(T));
}

}  // namespace sb::mem
