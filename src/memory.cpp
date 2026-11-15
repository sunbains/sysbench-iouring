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
#include "sysbench/layer_stats.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/numa.hpp"

#include <sys/mman.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <stdexcept>

namespace sb::mem {
namespace {

constexpr std::size_t kPageSize = 4096;
constexpr std::uint64_t kAlignedMagic = 0xA11CED0000000001ull;

struct alignas(16) AlignedHeader {
    std::uint64_t m_magic;
    void* m_block;
};

[[nodiscard]] constexpr bool is_power_of_two(std::size_t value) noexcept {
    return value != 0 && (value & (value - 1)) == 0;
}

[[nodiscard]] constexpr std::size_t align_up(std::size_t value,
                                             std::size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

[[nodiscard]] constexpr std::uint32_t saturating_u32(std::size_t value) noexcept {
    return value > std::numeric_limits<std::uint32_t>::max()
               ? std::numeric_limits<std::uint32_t>::max()
               : static_cast<std::uint32_t>(value);
}

}  // namespace

// ---------------------------------------------------------------------------
// RootArena
// ---------------------------------------------------------------------------

RootArena::RootArena(std::size_t capacity, bool map_populate)
    : m_capacity(align_up(capacity, kPageSize)), m_populated(map_populate) {
    if (m_capacity == 0) {
        throw std::invalid_argument("root arena capacity must be non-zero");
    }

    int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
    if (map_populate) flags |= MAP_POPULATE;

    void* mapping = ::mmap(nullptr, m_capacity, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (mapping == MAP_FAILED) {
        throw std::bad_alloc();
    }
    m_base = mapping;
#if defined(MADV_HUGEPAGE)
    // Best effort only: transparent huge pages reduce TLB pressure for the
    // large pre-reserved region.  Older kernels may return EINVAL.
    (void)::madvise(m_base, m_capacity, MADV_HUGEPAGE);
#endif
}

RootArena::~RootArena() {
    if (m_base != nullptr) {
        ::munmap(m_base, m_capacity);
        m_base = nullptr;
    }
}

void* RootArena::allocate(std::size_t bytes, std::size_t alignment) {
    if (bytes == 0) bytes = 1;
    if (!is_power_of_two(alignment) || alignment > (1U << 20)) {
        ++m_failures;
        throw std::bad_alloc();
    }

    const std::size_t aligned_bytes = align_up(bytes, kDefaultAlignment);
    std::size_t current = m_offset.load(std::memory_order_relaxed);
    for (;;) {
        const std::size_t misalignment =
            reinterpret_cast<std::uintptr_t>(static_cast<std::byte*>(m_base) + current) &
            (alignment - 1);
        const std::size_t padding = misalignment == 0 ? 0 : alignment - misalignment;
        if (current > m_capacity || padding > m_capacity - current ||
            aligned_bytes > m_capacity - current - padding) {
            ++m_failures;
            throw std::bad_alloc();
        }
        const std::size_t next = current + padding + aligned_bytes;
        if (m_offset.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
            ++m_allocations;
            std::size_t observed = m_peak.load(std::memory_order_relaxed);
            while (next > observed &&
                   !m_peak.compare_exchange_weak(observed, next,
                                                std::memory_order_relaxed)) {
            }
            return static_cast<std::byte*>(m_base) + current + padding;
        }
    }
}

bool RootArena::owns(const void* ptr) const noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(ptr);
    const auto begin = reinterpret_cast<std::uintptr_t>(m_base);
    return address >= begin && address < begin + m_capacity;
}

ResourceStats RootArena::stats() const noexcept {
    ResourceStats result;
    result.m_capacity = m_capacity;
    result.m_used = m_offset.load(std::memory_order_relaxed);
    result.m_peak = m_peak.load(std::memory_order_relaxed);
    result.m_allocations = m_allocations.load(std::memory_order_relaxed);
    result.m_frees = 0;
    result.m_failed = m_failures.load(std::memory_order_relaxed);
    return result;
}

// ---------------------------------------------------------------------------
// HeapArena
// ---------------------------------------------------------------------------

namespace {
static_assert(sizeof(HeapArena) > 0);
}

HeapArena::HeapArena(MemoryResource& parent, std::size_t bytes,
                     std::string_view name, int numa_node)
    : m_parent(&parent), m_name(name), m_numa_node(numa_node) {
    if (bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("heap arena too large (max 4 GiB)");
    }
    const std::size_t alignment = numa_node >= 0 ? 4096 : 16;
    m_capacity = align_up(bytes, alignment);
    if (m_capacity < kMinBlock) m_capacity = kMinBlock;
    m_base = static_cast<std::byte*>(parent.allocate(m_capacity, alignment));
    if (numa_node >= 0) {
        m_numa_bound = numa::bind_range(m_base, m_capacity, numa_node);
    }
    auto* initial = reinterpret_cast<Block*>(m_base);
    initial->m_size = static_cast<std::uint32_t>(m_capacity);
    initial->m_prev_size = 0;
    initial->m_is_free = 1;
    initial->m_bin = bin_for(m_capacity);
    initial->m_next_free = nullptr;
    initial->m_prev_free = nullptr;
    insert_free(initial);
}

HeapArena::~HeapArena() {
    if (m_base != nullptr) {
        m_parent->deallocate(m_base, m_capacity, 16);
    }
}

// Block sizes are multiples of 16 below 4 GiB.
std::uint32_t HeapArena::bin_for(std::size_t bytes) noexcept {
    if (bytes < kSmallLimit) return static_cast<std::uint32_t>(bytes >> 4);
    const unsigned level = 63u - static_cast<unsigned>(__builtin_clzll(bytes));
    const auto sub = static_cast<std::uint32_t>((bytes >> (level - kSubBinBits)) &
                                                ((1u << kSubBinBits) - 1));
    const std::uint32_t bin =
        static_cast<std::uint32_t>(kSmallBins) + ((level - 10) << kSubBinBits) + sub;
    return std::min<std::uint32_t>(bin, kBinCount - 1);
}

std::uint32_t HeapArena::search_bin(std::size_t bytes) noexcept {
    if (bytes < kSmallLimit) return static_cast<std::uint32_t>(bytes >> 4);
    const unsigned level = 63u - static_cast<unsigned>(__builtin_clzll(bytes));
    const std::size_t rounded = bytes + (std::size_t{1} << (level - kSubBinBits)) - 1;
    if (rounded >> 32 != 0) return static_cast<std::uint32_t>(kBinCount);
    return bin_for(rounded);
}

HeapArena::Block* HeapArena::block_of(void* ptr) const noexcept {
    return reinterpret_cast<Block*>(static_cast<std::byte*>(ptr) - sizeof(Block));
}

void* HeapArena::user_of(Block* block) const noexcept {
    return reinterpret_cast<std::byte*>(block) + sizeof(Block);
}

void HeapArena::insert_free(Block* block) noexcept {
    const std::uint32_t bin = bin_for(block->m_size);
    block->m_bin = bin;
    block->m_is_free = 1;
    block->m_prev_free = nullptr;
    Block* first = head(bin);
    block->m_next_free = first;
    if (first != nullptr) first->m_prev_free = block;
    set_head(bin, block);
    m_bin_map[bin / 64] |= std::uint64_t{1} << (bin % 64);
}

void HeapArena::remove_free(Block* block) noexcept {
    if (block->m_prev_free != nullptr) {
        block->m_prev_free->m_next_free = block->m_next_free;
    } else {
        set_head(block->m_bin, block->m_next_free);
        if (block->m_next_free == nullptr) {
            m_bin_map[block->m_bin / 64] &= ~(std::uint64_t{1} << (block->m_bin % 64));
        }
    }
    if (block->m_next_free != nullptr) block->m_next_free->m_prev_free = block->m_prev_free;
    block->m_next_free = nullptr;
    block->m_prev_free = nullptr;
    block->m_is_free = 0;
}

/* First the bins whose every block fits (a bitmap search, O(1)).  Only when
they are all empty, the request's own bin, whose blocks may or may not fit:
a nearly full heap then still finds the block a size-ordered search would. */
HeapArena::Block* HeapArena::find_fit(std::size_t wanted) noexcept {
    const std::uint32_t first = search_bin(wanted);
    if (first < kBinCount) {
        std::size_t word = first / 64;
        std::uint64_t bits = m_bin_map[word] & (~std::uint64_t{0} << (first % 64));
        while (bits == 0 && ++word < std::size(m_bin_map)) bits = m_bin_map[word];
        if (bits != 0) {
            return head(word * 64 + static_cast<std::size_t>(__builtin_ctzll(bits)));
        }
    }
    for (Block* block = head(bin_for(wanted)); block != nullptr; block = block->m_next_free) {
        if (block->m_size >= wanted) return block;
    }
    return nullptr;
}

void HeapArena::split(Block* block, std::size_t wanted) noexcept {
    if (block->m_size - wanted < kMinBlock) return;

    auto* remainder = reinterpret_cast<Block*>(reinterpret_cast<std::byte*>(block) +
                                               wanted);
    remainder->m_size = block->m_size - static_cast<std::uint32_t>(wanted);
    remainder->m_prev_size = static_cast<std::uint32_t>(wanted);
    remainder->m_is_free = 0;
    remainder->m_bin = 0;
    remainder->m_next_free = nullptr;
    remainder->m_prev_free = nullptr;

    block->m_size = static_cast<std::uint32_t>(wanted);

    std::byte* after = reinterpret_cast<std::byte*>(remainder) + remainder->m_size;
    if (after < m_base + m_capacity) {
        auto* next = reinterpret_cast<Block*>(after);
        next->m_prev_size = remainder->m_size;
    }
    insert_free(remainder);
}

void* HeapArena::allocate(std::size_t bytes, std::size_t alignment) {
    if (bytes == 0) bytes = 1;
    if (!is_power_of_two(alignment) || alignment > kMaxAlignment) {
        ++m_failures;
        throw std::bad_alloc();
    }

    const bool over_aligned = alignment > 16;
    const std::size_t extra = over_aligned
                                  ? alignment + 32  // aligned header + padding
                                  : 0;
    const std::size_t payload = align_up(bytes + extra, 16);
    if (payload > std::numeric_limits<std::uint32_t>::max() - sizeof(Block)) {
        ++m_failures;
        throw std::bad_alloc();
    }
    std::size_t wanted = sizeof(Block) + payload;
    if (wanted < kMinBlock) wanted = kMinBlock;

    Block* block = find_fit(wanted);
    if (block == nullptr) {
        ++m_failures;
        throw std::bad_alloc();
    }

    remove_free(block);
    split(block, wanted);
    block->m_is_free = 0;
    block->m_bin = kUsedBin;
    m_used += block->m_size;
    m_peak = std::max(m_peak, m_used);
    ++m_allocations;

    if (!over_aligned) return user_of(block);

    auto* raw = static_cast<std::byte*>(user_of(block));
    const auto aligned = reinterpret_cast<std::uintptr_t>(
        (reinterpret_cast<std::uintptr_t>(raw) + 32 + alignment - 1) &
        ~(std::uintptr_t(alignment) - 1));
    auto* header = reinterpret_cast<AlignedHeader*>(aligned - sizeof(AlignedHeader));
    header->m_magic = kAlignedMagic;
    header->m_block = block;
    return reinterpret_cast<void*>(aligned);
}

void HeapArena::deallocate(void* ptr, std::size_t, std::size_t alignment) noexcept {
    if (ptr == nullptr) return;
    Block* block = nullptr;
    if (alignment > 16) {
        auto* header = reinterpret_cast<AlignedHeader*>(
            static_cast<std::byte*>(ptr) - sizeof(AlignedHeader));
        if (header->m_magic != kAlignedMagic) return;
        block = static_cast<HeapArena::Block*>(header->m_block);
        header->m_magic = 0;
    } else {
        block = block_of(ptr);
    }
    if (block == nullptr || block->m_is_free != 0 || !owns(ptr)) {
        return;
    }

    m_used -= block->m_size;
    ++m_frees;
    block->m_is_free = 1;
    coalesce(block);
}

void HeapArena::coalesce(Block* block) noexcept {
    // Merge with previous block when it is free.
    if (block->m_prev_size != 0) {
        auto* previous = reinterpret_cast<Block*>(
            reinterpret_cast<std::byte*>(block) - block->m_prev_size);
        if (previous->m_is_free != 0) {
            remove_free(previous);
            previous->m_size += block->m_size;
            block = previous;
            std::byte* after = reinterpret_cast<std::byte*>(block) + block->m_size;
            if (after < m_base + m_capacity) {
                reinterpret_cast<Block*>(after)->m_prev_size = block->m_size;
            }
        }
    }

    // Merge with the following block when it is free.
    std::byte* after = reinterpret_cast<std::byte*>(block) + block->m_size;
    if (after < m_base + m_capacity) {
        auto* next = reinterpret_cast<Block*>(after);
        if (next->m_is_free != 0) {
            remove_free(next);
            block->m_size += next->m_size;
            std::byte* tail = reinterpret_cast<std::byte*>(block) + block->m_size;
            if (tail < m_base + m_capacity) {
                reinterpret_cast<Block*>(tail)->m_prev_size = block->m_size;
            }
        }
    }

    insert_free(block);
}

bool HeapArena::owns(const void* ptr) const noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(ptr);
    const auto begin = reinterpret_cast<std::uintptr_t>(m_base);
    return address >= begin && address < begin + m_capacity;
}

ResourceStats HeapArena::stats() const noexcept {
    ResourceStats result;
    result.m_capacity = m_capacity;
    result.m_used = m_used;
    result.m_peak = m_peak;
    result.m_allocations = m_allocations;
    result.m_frees = m_frees;
    result.m_failed = m_failures;
    return result;
}

void HeapArena::reset() noexcept {
    std::memset(m_bins, 0, sizeof(m_bins));
    std::memset(m_bin_map, 0, sizeof(m_bin_map));
    auto* initial = reinterpret_cast<Block*>(m_base);
    initial->m_size = saturating_u32(m_capacity);
    initial->m_prev_size = 0;
    initial->m_is_free = 1;
    initial->m_next_free = nullptr;
    initial->m_prev_free = nullptr;
    insert_free(initial);
    m_used = 0;
    m_allocations = 0;
    m_frees = 0;
    m_failures = 0;
}

// ---------------------------------------------------------------------------
// BumpArena
// ---------------------------------------------------------------------------

BumpArena::BumpArena(MemoryResource& parent, std::size_t block_size,
                     std::string_view name)
    : m_parent(&parent), m_block_size(std::max(block_size, kHeaderAligned + 64)),
      m_name(name) {
    // Keep one baseline block so per-event scopes only recycle bump space and
    // never hit the worker heap in steady state.
    (void)new_block(0);
}

BumpArena::~BumpArena() {
    Block* block = m_head;
    while (block != nullptr) {
        Block* next = block->m_next;
        m_parent->deallocate(block, block->m_capacity + kHeaderAligned, 16);
        block = next;
    }
    m_head = nullptr;
    m_current = nullptr;
}

BumpArena::Block* BumpArena::new_block(std::size_t payload) {
    payload = std::max(payload, m_block_size);
    payload = align_up(payload, 16);
    const std::size_t total = kHeaderAligned + payload;
    void* memory = m_parent->allocate(total, 16);
    auto* block = static_cast<Block*>(memory);
    block->m_next = nullptr;
    block->m_capacity = payload;
    block->m_used = 0;
    if (m_current != nullptr) {
        m_current->m_next = block;
    } else {
        m_head = block;
    }
    m_current = block;
    m_total_capacity += payload;
    return block;
}

void* BumpArena::allocate(std::size_t bytes, std::size_t alignment) {
    if (bytes == 0) bytes = 1;
    if (!is_power_of_two(alignment)) {
        ++m_failures;
        throw std::bad_alloc();
    }

    if (m_current == nullptr) {
        (void)new_block(bytes + alignment);
    }

    for (;;) {
        const auto base = reinterpret_cast<std::uintptr_t>(payload_of(m_current));
        const std::uintptr_t current_address = base + m_current->m_used;
        const std::uintptr_t aligned_address =
            (current_address + alignment - 1) & ~(std::uintptr_t(alignment) - 1);
        const std::size_t offset = aligned_address - base;
        if (offset <= m_current->m_capacity && bytes <= m_current->m_capacity - offset) {
            m_current->m_used = offset + bytes;
            m_used += bytes;
            m_peak = std::max(m_peak, m_used);
            ++m_allocations;
            return reinterpret_cast<void*>(aligned_address);
        }

        (void)new_block(bytes + alignment);
    }
}

bool BumpArena::owns(const void* ptr) const noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(ptr);
    for (Block* block = m_head; block != nullptr; block = block->m_next) {
        const auto begin = reinterpret_cast<std::uintptr_t>(payload_of(block));
        if (address >= begin && address < begin + block->m_capacity) return true;
    }
    return false;
}

ResourceStats BumpArena::stats() const noexcept {
    ResourceStats result;
    result.m_capacity = m_total_capacity;
    result.m_used = m_used;
    result.m_peak = m_peak;
    result.m_allocations = m_allocations;
    result.m_frees = 0;
    result.m_failed = m_failures;
    return result;
}

BumpArena::Mark BumpArena::mark() const noexcept {
    return Mark{m_current, m_current != nullptr ? m_current->m_used : 0, m_generation};
}

void BumpArena::rewind(Mark mark) noexcept {
    if (mark.m_block == nullptr) {
        Block* block = m_head;
        while (block != nullptr) {
            Block* next = block->m_next;
            m_parent->deallocate(block, block->m_capacity + kHeaderAligned, 16);
            block = next;
        }
        m_head = nullptr;
        m_current = nullptr;
        m_total_capacity = 0;
        m_used = 0;
        return;
    }

    auto* target = static_cast<Block*>(mark.m_block);
    if (target->m_used > mark.m_used) target->m_used = mark.m_used;

    Block* block = target->m_next;
    while (block != nullptr) {
        Block* next = block->m_next;
        m_parent->deallocate(block, block->m_capacity + kHeaderAligned, 16);
        m_total_capacity -= block->m_capacity;
        block = next;
    }
    target->m_next = nullptr;
    m_current = target;

    std::size_t used = 0;
    std::size_t capacity = 0;
    for (Block* it = m_head; it != nullptr; it = it->m_next) {
        used += it->m_used;
        capacity += it->m_capacity;
    }
    m_used = used;
    m_total_capacity = capacity;
}

// ---------------------------------------------------------------------------
// GlobalAllocator
// ---------------------------------------------------------------------------

GlobalAllocator& GlobalAllocator::instance() {
    static GlobalAllocator allocator;
    return allocator;
}

void GlobalAllocator::initialize(std::size_t capacity, bool map_populate) {
    if (m_root != nullptr) {
        throw std::logic_error("global allocator already initialized");
    }
    m_root = ::new (static_cast<void*>(m_storage)) RootArena(capacity, map_populate);
}

void GlobalAllocator::initialize_if_needed(std::size_t capacity,
                                           bool map_populate) {
    if (m_root == nullptr) initialize(capacity, map_populate);
}

RootArena& GlobalAllocator::root() {
    if (m_root == nullptr) {
        throw std::logic_error("global allocator is not initialized");
    }
    return *m_root;
}

const RootArena& GlobalAllocator::root() const {
    if (m_root == nullptr) {
        throw std::logic_error("global allocator is not initialized");
    }
    return *m_root;
}

// ---------------------------------------------------------------------------
// Per-NUMA-node coroutine frame pool
// ---------------------------------------------------------------------------

namespace {

constexpr std::size_t kFrameSlabSize = 1024 * 1024;
constexpr std::size_t kFrameReserve = 64;

struct alignas(16) FrameHeader {
    std::uint32_t m_class_index;
    std::uint32_t m_node;
    std::uint32_t m_magic;
    std::uint32_t m_reserved;
    FrameHeader* m_next;
};

constexpr std::uint32_t kFrameAlive = 0xA11CF00Du;
constexpr std::uint32_t kFrameFree = 0xF4EEF4EEu;
constexpr std::size_t kFrameGranularity = 64;
constexpr std::size_t kMaxSmallFrame = 8192;
constexpr std::size_t kSmallClasses = kMaxSmallFrame / kFrameGranularity;
constexpr std::size_t kLargeClasses = 24;
constexpr std::size_t kFrameClasses = kSmallClasses + kLargeClasses;

// x86-64 user addresses fit in the low 48 bits; the top 16 bits carry an ABA
// tag.  A shared frame free-list is a Treiber stack updated with one 64-bit CAS.
constexpr std::uint64_t kFramePointerMask = (1ull << 48) - 1;
constexpr unsigned kFrameTagShift = 48;

struct alignas(64) FrameStack {
    std::atomic<std::uint64_t> m_head{0};
};

std::array<std::array<FrameStack, kFrameClasses>, numa::kMaxNodes>& frame_stacks() {
    static std::array<std::array<FrameStack, kFrameClasses>, numa::kMaxNodes> stacks;
    return stacks;
}

struct FrameSlab {
    std::byte* m_base = nullptr;
    std::size_t m_capacity = 0;
    std::size_t m_used = 0;
};

thread_local int tls_frame_node = 0;

// The coroutine frame allocator.  Coroutines are pinned to the worker that
// created them, so a frame is almost always freed on the thread that allocated
// it.  Each thread keeps its own per-class free lists: allocation and free are
// a pointer pop and push with no atomics and no shared cache line.  A frame
// carries its owner's token; one freed on another thread (rare) goes to the
// per-node shared stack, from which an owner refills only when its own list
// and slab are both exhausted.
std::atomic<std::uint32_t> g_next_frame_owner{1};
thread_local std::uint32_t tls_frame_owner = 0;

std::uint32_t frame_owner() noexcept {
    if (tls_frame_owner == 0)
        tls_frame_owner = g_next_frame_owner.fetch_add(1, std::memory_order_relaxed);
    return tls_frame_owner;
}
// Frames are recycled on the thread that frees them.  A query creates and
// destroys several frames of one class per resume (destruction is deferred until
// the resume unwinds), so a small cache overflowed into the per-node shared
// stack on every query and every worker of the node fought over its head.
constexpr std::size_t kFrameCachePerClass = 1024;
thread_local FrameHeader* tls_frame_cache[kFrameClasses]{};
thread_local std::uint16_t tls_frame_cache_count[kFrameClasses]{};
thread_local std::array<FrameSlab, numa::kMaxNodes> tls_frame_slabs{};

FrameStack& frame_stack_for(int node, std::size_t index) noexcept {
    const std::size_t safe_node =
        node >= 0 && static_cast<std::uint32_t>(node) < numa::kMaxNodes
            ? static_cast<std::size_t>(node)
            : 0;
    return frame_stacks()[safe_node][index];
}

std::size_t frame_class(std::size_t size) noexcept {
    if (size <= kFrameGranularity) return 0;
    if (size <= kMaxSmallFrame) {
        return (size + kFrameGranularity - 1) / kFrameGranularity - 1;
    }
    std::size_t capacity = kMaxSmallFrame;
    for (std::size_t index = kSmallClasses; index < kFrameClasses; ++index) {
        capacity *= 2;
        if (size <= capacity) return index;
    }
    return kFrameClasses - 1;
}

std::size_t frame_capacity(std::size_t index) noexcept {
    if (index < kSmallClasses) return (index + 1) * kFrameGranularity;
    return kMaxSmallFrame << (index - kSmallClasses + 1);
}

void push_frame(FrameStack& stack, FrameHeader* header) noexcept {
    std::uint64_t old = stack.m_head.load(std::memory_order_acquire);
    for (;;) {
        const std::uint64_t tag =
            ((old >> kFrameTagShift) + 1) & 0xffffu;
        const std::uint64_t address =
            reinterpret_cast<std::uintptr_t>(header) & kFramePointerMask;
        const std::uint64_t desired = (tag << kFrameTagShift) | address;
        header->m_next = reinterpret_cast<FrameHeader*>(old & kFramePointerMask);
        if (stack.m_head.compare_exchange_weak(old, desired,
                                             std::memory_order_release,
                                             std::memory_order_acquire)) {
            return;
        }
    }
}

FrameHeader* pop_frame(FrameStack& stack) noexcept {
    std::uint64_t old = stack.m_head.load(std::memory_order_acquire);
    for (;;) {
        auto* header = reinterpret_cast<FrameHeader*>(old & kFramePointerMask);
        if (header == nullptr) return nullptr;
        const std::uint64_t tag =
            ((old >> kFrameTagShift) + 1) & 0xffffu;
        const std::uint64_t desired =
            (tag << kFrameTagShift) |
            (reinterpret_cast<std::uintptr_t>(header->m_next) & kFramePointerMask);
        if (stack.m_head.compare_exchange_weak(old, desired,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            header->m_next = nullptr;
            return header;
        }
    }
}

}  // namespace

int thread_frame_node() noexcept {
    return tls_frame_node;
}

void set_thread_frame_node(int node) noexcept {
    if (node == tls_frame_node) return;
    const int previous = tls_frame_node;
    for (std::size_t index = 0; index < kFrameClasses; ++index) {
        FrameHeader* header = tls_frame_cache[index];
        tls_frame_cache[index] = nullptr;
        tls_frame_cache_count[index] = 0;
        FrameStack& stack = frame_stack_for(previous, index);
        while (header != nullptr) {
            FrameHeader* next = header->m_next;
            push_frame(stack, header);
            header = next;
        }
    }
    tls_frame_node = node;
}

void* allocate_task_frame(std::size_t size) {
    SB_LAYER(Memory);
    const int node = tls_frame_node;
    const std::size_t index = frame_class(size);
    const std::uint32_t owner = frame_owner();

    if (FrameHeader* header = tls_frame_cache[index]; header != nullptr) [[likely]] {
        tls_frame_cache[index] = header->m_next;
        header->m_next = nullptr;
        header->m_magic = kFrameAlive;
        header->m_reserved = owner;
        return reinterpret_cast<std::byte*>(header) + kFrameReserve;
    }

    const std::size_t capacity = frame_capacity(index);
    const std::size_t need = (capacity + kFrameReserve + 63) & ~std::size_t(63);
    const std::size_t safe_node =
        node >= 0 && static_cast<std::uint32_t>(node) < numa::kMaxNodes
            ? static_cast<std::size_t>(node)
            : 0;

    /* Frames another thread gave back for this node and class. */
    if (FrameHeader* header = pop_frame(frame_stack_for(node, index));
        header != nullptr) {
        header->m_next = nullptr;
        header->m_magic = kFrameAlive;
        header->m_reserved = owner;
        return reinterpret_cast<std::byte*>(header) + kFrameReserve;
    }

    FrameSlab& slab = tls_frame_slabs[safe_node];
    if (slab.m_base == nullptr || slab.m_used + need > slab.m_capacity) {
        const std::size_t slab_size = align_up(std::max(kFrameSlabSize, need), 4096);
        auto* base = static_cast<std::byte*>(global_root().allocate(slab_size, 4096));
        if (node >= 0 && static_cast<std::uint32_t>(node) < numa::kMaxNodes) {
            (void)numa::bind_range(base, slab_size, node);
        }
        slab.m_base = base;
        slab.m_capacity = slab_size;
        slab.m_used = 0;
    }

    auto* header = reinterpret_cast<FrameHeader*>(slab.m_base + slab.m_used);
    slab.m_used += need;
    header->m_class_index = static_cast<std::uint32_t>(index);
    header->m_node = static_cast<std::uint32_t>(safe_node);
    header->m_magic = kFrameAlive;
    header->m_reserved = owner;
    header->m_next = nullptr;
    return reinterpret_cast<std::byte*>(header) + kFrameReserve;
}

void deallocate_task_frame(void* pointer, std::size_t) noexcept {
    SB_LAYER(Memory);
    if (pointer == nullptr) return;
    auto* header = reinterpret_cast<FrameHeader*>(
        static_cast<std::byte*>(pointer) - kFrameReserve);
    if (header->m_magic != kFrameAlive) {
        std::println(stderr, "sysbench: coroutine frame pool corruption (ptr={:p} magic={:x})",
                     pointer, header->m_magic);
        std::abort();
    }
    header->m_magic = kFrameFree;
    const std::size_t index = header->m_class_index;
    if (index >= kFrameClasses) return;
    if (header->m_reserved == tls_frame_owner &&
        static_cast<int>(header->m_node) == tls_frame_node) [[likely]] {
        header->m_next = tls_frame_cache[index];
        tls_frame_cache[index] = header;
        return;
    }
    FrameStack& stack = frame_stack_for(static_cast<int>(header->m_node), index);
    push_frame(stack, header);
}

}  // namespace sb::mem
