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

// NIC queue ownership for a client socket.
//
// A multiqueue NIC has one channel per CPU: a TX queue, an RX queue and the
// NAPI instance (with its interrupt) that serves both.  A worker pinned to a
// CPU transmits on that CPU's TX queue (XPS); if the replies to its
// connections also arrive on that channel, the whole connection (send, TX
// completion, receive, TCP processing, the read) stays on the worker's CPU
// and the worker's busy poll covers both directions.  RSS picks the RX queue
// from the connection's 4-tuple, so the client steers it by choosing its
// local port: connect, compare SO_INCOMING_NAPI_ID with the worker's channel,
// and reconnect if it differs.
//
// Everything here is read without privileges: XPS maps from sysfs, the
// queue-to-NAPI map from the netdev netlink family, IRQ affinity from /proc.

#include <cstdint>

namespace sb::nic {

// NAPI id of the channel that `cpu` transmits on, on the interface that
// carries the connected socket `fd`'s traffic.  0 when unknown (no
// multiqueue NIC, no XPS or netdev netlink, loopback).
[[nodiscard]] std::uint32_t napi_for_cpu(int fd, int cpu) noexcept;

// NAPI id that received the socket's latest packet (SO_INCOMING_NAPI_ID).
[[nodiscard]] std::uint32_t incoming_napi(int fd) noexcept;

// Number of channels (RX queues) of the socket's interface; 0 if unknown.
[[nodiscard]] std::uint32_t channel_count(int fd) noexcept;

}  // namespace sb::nic
