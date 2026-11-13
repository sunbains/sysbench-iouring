#include <print>
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
    std::uint64_t magic;
    void* block;
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
    result.capacity = m_capacity;
    result.used = m_offset.load(std::memory_order_relaxed);
    result.peak = m_peak.load(std::memory_order_relaxed);
    result.allocations = m_allocations.load(std::memory_order_relaxed);
    result.frees = 0;
    result.failed = m_failures.load(std::memory_order_relaxed);
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
    initial->size = static_cast<std::uint32_t>(m_capacity);
    initial->prev_size = 0;
    initial->is_free = 1;
    initial->bin = bin_for(m_capacity);
    initial->next_free = nullptr;
    initial->prev_free = nullptr;
    insert_free(initial);
}

HeapArena::~HeapArena() {
    if (m_base != nullptr) {
        m_parent->deallocate(m_base, m_capacity, 16);
    }
}

std::uint32_t HeapArena::bin_for(std::size_t bytes) noexcept {
    if (bytes <= kMinBlock) return 0;
    unsigned int log = 0;
    std::size_t value = bytes - 1;
    while (value != 0) {
        value >>= 1;
        ++log;
    }
    return std::min<std::uint32_t>(log - 5, kBinCount - 1);
}

HeapArena::Block* HeapArena::block_of(void* ptr) const noexcept {
    return reinterpret_cast<Block*>(static_cast<std::byte*>(ptr) - sizeof(Block));
}

void* HeapArena::user_of(Block* block) const noexcept {
    return reinterpret_cast<std::byte*>(block) + sizeof(Block);
}

void HeapArena::insert_free(Block* block) noexcept {
    const std::uint32_t bin = bin_for(block->size);
    block->bin = bin;
    block->is_free = 1;
    block->prev_free = nullptr;
    block->next_free = m_bins[bin];
    if (m_bins[bin] != nullptr) m_bins[bin]->prev_free = block;
    m_bins[bin] = block;
}

void HeapArena::remove_free(Block* block) noexcept {
    if (block->prev_free != nullptr) {
        block->prev_free->next_free = block->next_free;
    } else {
        m_bins[block->bin] = block->next_free;
    }
    if (block->next_free != nullptr) block->next_free->prev_free = block->prev_free;
    block->next_free = nullptr;
    block->prev_free = nullptr;
    block->is_free = 0;
}

HeapArena::Block* HeapArena::find_fit(std::size_t wanted) noexcept {
    const std::uint32_t first = bin_for(wanted);
    for (std::uint32_t bin = first; bin < kBinCount; ++bin) {
        Block* block = m_bins[bin];
        while (block != nullptr) {
            if (block->size >= wanted) return block;
            block = block->next_free;
        }
    }
    return nullptr;
}

void HeapArena::split(Block* block, std::size_t wanted) noexcept {
    if (block->size - wanted < kMinBlock) return;

    auto* remainder = reinterpret_cast<Block*>(reinterpret_cast<std::byte*>(block) +
                                               wanted);
    remainder->size = block->size - static_cast<std::uint32_t>(wanted);
    remainder->prev_size = static_cast<std::uint32_t>(wanted);
    remainder->is_free = 0;
    remainder->bin = 0;
    remainder->next_free = nullptr;
    remainder->prev_free = nullptr;

    block->size = static_cast<std::uint32_t>(wanted);

    std::byte* after = reinterpret_cast<std::byte*>(remainder) + remainder->size;
    if (after < m_base + m_capacity) {
        auto* next = reinterpret_cast<Block*>(after);
        next->prev_size = remainder->size;
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
    block->is_free = 0;
    block->bin = kUsedBin;
    m_used += block->size;
    m_peak = std::max(m_peak, m_used);
    ++m_allocations;

    if (!over_aligned) return user_of(block);

    auto* raw = static_cast<std::byte*>(user_of(block));
    const auto aligned = reinterpret_cast<std::uintptr_t>(
        (reinterpret_cast<std::uintptr_t>(raw) + 32 + alignment - 1) &
        ~(std::uintptr_t(alignment) - 1));
    auto* header = reinterpret_cast<AlignedHeader*>(aligned - sizeof(AlignedHeader));
    header->magic = kAlignedMagic;
    header->block = block;
    return reinterpret_cast<void*>(aligned);
}

void HeapArena::deallocate(void* ptr, std::size_t, std::size_t alignment) noexcept {
    if (ptr == nullptr) return;
    Block* block = nullptr;
    if (alignment > 16) {
        auto* header = reinterpret_cast<AlignedHeader*>(
            static_cast<std::byte*>(ptr) - sizeof(AlignedHeader));
        if (header->magic != kAlignedMagic) return;
        block = static_cast<HeapArena::Block*>(header->block);
        header->magic = 0;
    } else {
        block = block_of(ptr);
    }
    if (block == nullptr || block->is_free != 0 || !owns(ptr)) {
        return;
    }

    m_used -= block->size;
    ++m_frees;
    block->is_free = 1;
    coalesce(block);
}

void HeapArena::coalesce(Block* block) noexcept {
    // Merge with previous block when it is free.
    if (block->prev_size != 0) {
        auto* previous = reinterpret_cast<Block*>(
            reinterpret_cast<std::byte*>(block) - block->prev_size);
        if (previous->is_free != 0) {
            remove_free(previous);
            previous->size += block->size;
            block = previous;
            std::byte* after = reinterpret_cast<std::byte*>(block) + block->size;
            if (after < m_base + m_capacity) {
                reinterpret_cast<Block*>(after)->prev_size = block->size;
            }
        }
    }

    // Merge with the following block when it is free.
    std::byte* after = reinterpret_cast<std::byte*>(block) + block->size;
    if (after < m_base + m_capacity) {
        auto* next = reinterpret_cast<Block*>(after);
        if (next->is_free != 0) {
            remove_free(next);
            block->size += next->size;
            std::byte* tail = reinterpret_cast<std::byte*>(block) + block->size;
            if (tail < m_base + m_capacity) {
                reinterpret_cast<Block*>(tail)->prev_size = block->size;
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
    result.capacity = m_capacity;
    result.used = m_used;
    result.peak = m_peak;
    result.allocations = m_allocations;
    result.frees = m_frees;
    result.failed = m_failures;
    return result;
}

void HeapArena::reset() noexcept {
    std::memset(m_bins, 0, sizeof(m_bins));
    auto* initial = reinterpret_cast<Block*>(m_base);
    initial->size = saturating_u32(m_capacity);
    initial->prev_size = 0;
    initial->is_free = 1;
    initial->next_free = nullptr;
    initial->prev_free = nullptr;
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
        Block* next = block->next;
        m_parent->deallocate(block, block->capacity + kHeaderAligned, 16);
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
    block->next = nullptr;
    block->capacity = payload;
    block->used = 0;
    if (m_current != nullptr) {
        m_current->next = block;
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
        const std::uintptr_t current_address = base + m_current->used;
        const std::uintptr_t aligned_address =
            (current_address + alignment - 1) & ~(std::uintptr_t(alignment) - 1);
        const std::size_t offset = aligned_address - base;
        if (offset <= m_current->capacity && bytes <= m_current->capacity - offset) {
            m_current->used = offset + bytes;
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
    for (Block* block = m_head; block != nullptr; block = block->next) {
        const auto begin = reinterpret_cast<std::uintptr_t>(payload_of(block));
        if (address >= begin && address < begin + block->capacity) return true;
    }
    return false;
}

ResourceStats BumpArena::stats() const noexcept {
    ResourceStats result;
    result.capacity = m_total_capacity;
    result.used = m_used;
    result.peak = m_peak;
    result.allocations = m_allocations;
    result.frees = 0;
    result.failed = m_failures;
    return result;
}

BumpArena::Mark BumpArena::mark() const noexcept {
    return Mark{m_current, m_current != nullptr ? m_current->used : 0, m_generation};
}

void BumpArena::rewind(Mark mark) noexcept {
    if (mark.block == nullptr) {
        Block* block = m_head;
        while (block != nullptr) {
            Block* next = block->next;
            m_parent->deallocate(block, block->capacity + kHeaderAligned, 16);
            block = next;
        }
        m_head = nullptr;
        m_current = nullptr;
        m_total_capacity = 0;
        m_used = 0;
        return;
    }

    auto* target = static_cast<Block*>(mark.block);
    if (target->used > mark.used) target->used = mark.used;

    Block* block = target->next;
    while (block != nullptr) {
        Block* next = block->next;
        m_parent->deallocate(block, block->capacity + kHeaderAligned, 16);
        m_total_capacity -= block->capacity;
        block = next;
    }
    target->next = nullptr;
    m_current = target;

    std::size_t used = 0;
    std::size_t capacity = 0;
    for (Block* it = m_head; it != nullptr; it = it->next) {
        used += it->used;
        capacity += it->capacity;
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
    std::uint32_t class_index;
    std::uint32_t node;
    std::uint32_t magic;
    std::uint32_t reserved;
    FrameHeader* next;
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
    std::atomic<std::uint64_t> head{0};
};

std::array<std::array<FrameStack, kFrameClasses>, numa::kMaxNodes>& frame_stacks() {
    static std::array<std::array<FrameStack, kFrameClasses>, numa::kMaxNodes> stacks;
    return stacks;
}

struct FrameSlab {
    std::byte* base = nullptr;
    std::size_t capacity = 0;
    std::size_t used = 0;
};

thread_local int tls_frame_node = 0;
constexpr std::size_t kFrameCachePerClass = 8;
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
    std::uint64_t old = stack.head.load(std::memory_order_acquire);
    for (;;) {
        const std::uint64_t tag =
            ((old >> kFrameTagShift) + 1) & 0xffffu;
        const std::uint64_t address =
            reinterpret_cast<std::uintptr_t>(header) & kFramePointerMask;
        const std::uint64_t desired = (tag << kFrameTagShift) | address;
        header->next = reinterpret_cast<FrameHeader*>(old & kFramePointerMask);
        if (stack.head.compare_exchange_weak(old, desired,
                                             std::memory_order_release,
                                             std::memory_order_acquire)) {
            return;
        }
    }
}

FrameHeader* pop_frame(FrameStack& stack) noexcept {
    std::uint64_t old = stack.head.load(std::memory_order_acquire);
    for (;;) {
        auto* header = reinterpret_cast<FrameHeader*>(old & kFramePointerMask);
        if (header == nullptr) return nullptr;
        const std::uint64_t tag =
            ((old >> kFrameTagShift) + 1) & 0xffffu;
        const std::uint64_t desired =
            (tag << kFrameTagShift) |
            (reinterpret_cast<std::uintptr_t>(header->next) & kFramePointerMask);
        if (stack.head.compare_exchange_weak(old, desired,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            header->next = nullptr;
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
            FrameHeader* next = header->next;
            push_frame(stack, header);
            header = next;
        }
    }
    tls_frame_node = node;
}

void* allocate_task_frame(std::size_t size) {
    const int node = tls_frame_node;
    const std::size_t index = frame_class(size);
    const std::size_t capacity = frame_capacity(index);
    const std::size_t need = (capacity + kFrameReserve + 63) & ~std::size_t(63);

    if (tls_frame_cache[index] != nullptr) {
        FrameHeader* header = tls_frame_cache[index];
        tls_frame_cache[index] = header->next;
        --tls_frame_cache_count[index];
        header->next = nullptr;
        header->magic = kFrameAlive;
        return reinterpret_cast<std::byte*>(header) + kFrameReserve;
    }

    FrameStack& stack = frame_stack_for(node, index);
    for (std::size_t i = 0; i < kFrameCachePerClass; ++i) {
        FrameHeader* header = pop_frame(stack);
        if (header == nullptr) break;
        header->next = tls_frame_cache[index];
        tls_frame_cache[index] = header;
        ++tls_frame_cache_count[index];
    }
    if (tls_frame_cache[index] != nullptr) {
        FrameHeader* header = tls_frame_cache[index];
        tls_frame_cache[index] = header->next;
        --tls_frame_cache_count[index];
        header->next = nullptr;
        header->magic = kFrameAlive;
        return reinterpret_cast<std::byte*>(header) + kFrameReserve;
    }

    const std::size_t safe_node =
        node >= 0 && static_cast<std::uint32_t>(node) < numa::kMaxNodes
            ? static_cast<std::size_t>(node)
            : 0;
    FrameSlab& slab = tls_frame_slabs[safe_node];
    if (slab.base == nullptr || slab.used + need > slab.capacity) {
        const std::size_t slab_size = align_up(std::max(kFrameSlabSize, need), 4096);
        auto* base = static_cast<std::byte*>(global_root().allocate(slab_size, 4096));
        if (node >= 0 && static_cast<std::uint32_t>(node) < numa::kMaxNodes) {
            (void)numa::bind_range(base, slab_size, node);
        }
        slab.base = base;
        slab.capacity = slab_size;
        slab.used = 0;
    }

    auto* header = reinterpret_cast<FrameHeader*>(slab.base + slab.used);
    slab.used += need;
    header->class_index = static_cast<std::uint32_t>(index);
    header->node = static_cast<std::uint32_t>(safe_node);
    header->magic = kFrameAlive;
    header->reserved = 0;
    header->next = nullptr;
    return reinterpret_cast<std::byte*>(header) + kFrameReserve;
}

void deallocate_task_frame(void* pointer, std::size_t) noexcept {
    if (pointer == nullptr) return;
    auto* header = reinterpret_cast<FrameHeader*>(
        static_cast<std::byte*>(pointer) - kFrameReserve);
    if (header->magic != kFrameAlive) {
        std::println(stderr, "sysbench: coroutine frame pool corruption (ptr={:p} magic={:x})",
                     pointer, header->magic);
        std::abort();
    }
    header->magic = kFrameFree;
    const std::size_t index = header->class_index;
    if (index >= kFrameClasses) return;
    if (static_cast<int>(header->node) == tls_frame_node &&
        tls_frame_cache_count[index] < kFrameCachePerClass) {
        header->next = tls_frame_cache[index];
        tls_frame_cache[index] = header;
        ++tls_frame_cache_count[index];
        return;
    }
    FrameStack& stack = frame_stack_for(static_cast<int>(header->node), index);
    push_frame(stack, header);
}

}  // namespace sb::mem
