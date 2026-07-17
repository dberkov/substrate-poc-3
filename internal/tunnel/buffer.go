// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel

import (
	"errors"
	"fmt"
)

// ErrFull is returned by Append when the buffer would exceed its cap. The
// caller applies backpressure by not reading its source until TrimTo frees
// space — TCP flow control then pushes back on the sender.
var ErrFull = errors.New("tunnel: replay buffer full")

// ReplayBuffer retains one direction of a session's un-acknowledged bytes,
// addressed by cumulative stream offset. The producer Appends as bytes are
// forwarded, TrimTo drops bytes the peer has confirmed, and From replays
// the tail after a reconnect. Not goroutine-safe; callers hold the session
// lock.
//
// Because the sidecar's buffers live inside the actor they are checkpointed
// with everything else — a suspend can never lose un-acked bytes by
// construction.
type ReplayBuffer struct {
	base uint64 // stream offset of buf[0]
	buf  []byte
	max  int
}

func NewReplayBuffer(max int) *ReplayBuffer {
	return &ReplayBuffer{max: max}
}

// Append retains p (already forwarded to the peer, or about to be).
func (b *ReplayBuffer) Append(p []byte) error {
	if len(b.buf)+len(p) > b.max {
		return ErrFull
	}
	b.buf = append(b.buf, p...)
	return nil
}

// Free reports how many more bytes Append can take.
func (b *ReplayBuffer) Free() int { return b.max - len(b.buf) }

// Len reports the retained (un-acked) byte count.
func (b *ReplayBuffer) Len() int { return len(b.buf) }

// Base is the offset of the first retained byte.
func (b *ReplayBuffer) Base() uint64 { return b.base }

// End is the offset one past the last retained byte — i.e. the total number
// of bytes ever appended.
func (b *ReplayBuffer) End() uint64 { return b.base + uint64(len(b.buf)) }

// TrimTo drops all bytes below offset (peer confirmed them). Offsets at or
// below base are a no-op; offsets beyond End are clamped.
func (b *ReplayBuffer) TrimTo(offset uint64) {
	if offset <= b.base {
		return
	}
	if offset >= b.End() {
		b.base = b.End()
		b.buf = b.buf[:0]
		return
	}
	n := offset - b.base
	// Copy down rather than re-slice so the backing array doesn't pin the
	// trimmed prefix forever.
	remaining := copy(b.buf, b.buf[n:])
	b.buf = b.buf[:remaining]
	b.base = offset
}

// From returns the retained bytes starting at offset, for replay. The
// returned slice aliases the buffer — consume it before the next Append or
// TrimTo. An offset below base means the peer asked for bytes we already
// dropped, which is a protocol violation (they were acked).
func (b *ReplayBuffer) From(offset uint64) ([]byte, error) {
	if offset < b.base {
		return nil, fmt.Errorf("tunnel: replay from %d but buffer starts at %d (already acked)", offset, b.base)
	}
	if offset > b.End() {
		return nil, fmt.Errorf("tunnel: replay from %d beyond end %d", offset, b.End())
	}
	return b.buf[offset-b.base:], nil
}
