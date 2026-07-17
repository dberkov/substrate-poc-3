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

package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/dberkov/substrate-poc-3/internal/tunnel"
)

// session is one agent-side connection, terminated at the broker. It holds:
//
//   - upstream: the real destination connection, dialed once and kept open
//     across every actor suspend, so the destination never sees a break.
//   - downBuf: upstream→agent bytes the broker has produced but the sidecar
//     has not yet acked. Replayed from the sidecar's delivered offset after
//     a reconnect. This is the buffer that must survive a suspend.
//   - upstreamWritten: agent→upstream bytes already written to upstream, so
//     replayed DATA after a reconnect is de-duplicated by offset.
//
// The wake policy (DESIGN.md §7) lives in produceDownstream / closeUpstream:
// while detached, a pending stream (a request went out, no response yet)
// wakes the actor on inbound data or an upstream close; a quiescent stream
// (idle keep-alive) does not.
type session struct {
	b         *Broker
	actorID   string
	sessionID uint64
	target    string
	log       *slog.Logger

	mu        sync.Mutex
	cond      *sync.Cond // signals downBuf space freed (backpressure) or bind change
	tconn     *tunnelConn
	upstream  net.Conn
	connected bool   // upstream dial completed
	preConn   []byte // agent→upstream bytes received before the dial completed

	downBuf         *tunnel.ReplayBuffer // upstream→agent, produced by broker
	upstreamWritten uint64               // agent→upstream bytes committed to upstream

	// requestOutstanding is true from when the agent writes request bytes
	// upstream until the whole response has been delivered AND acked by the
	// sidecar (downBuf drains to empty). It distinguishes a mid-request
	// suspend (wake on upstream close) from an idle keep-alive reaped by the
	// server (do not wake). Wake never depends on tunnel write success — a
	// write to a just-suspended peer can succeed into a dead socket before
	// any RST — only on buffered/acked state and explicit detach.
	requestOutstanding bool

	upClosed   bool // agent→upstream half closed (sidecar sent CLOSE DirUp)
	downClosed bool // upstream→agent half closed (upstream EOF/error)
	dead       bool
	createdAt  time.Time
	watchdog   *time.Timer
}

func newSession(b *Broker, actorID string, sessionID uint64, target string) *session {
	s := &session{
		b:         b,
		actorID:   actorID,
		sessionID: sessionID,
		target:    target,
		log:       b.log.With("actor", actorID, "sid", sessionID, "target", target),
		downBuf:   tunnel.NewReplayBuffer(b.cfg.SessionBufferBytes),
		createdAt: time.Now(),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// bind attaches the session to a (new) tunnel connection and registers it
// so a tunnel drop detaches it.
func (s *session) bind(tc *tunnelConn) {
	s.mu.Lock()
	s.tconn = tc
	s.mu.Unlock()
	tc.register(s)
	s.cond.Broadcast()
}

// detach unbinds from tc if it is still the bound connection. The upstream
// and buffers are left intact for the next ATTACH. If undelivered downstream
// data (or a pending close) is waiting, the actor is woken so it re-attaches
// — this is the reliable wake path: it does not depend on whether an earlier
// optimistic tunnel write appeared to succeed.
func (s *session) detach(tc *tunnelConn) {
	s.mu.Lock()
	if s.tconn != tc {
		s.mu.Unlock()
		return
	}
	s.tconn = nil
	wake := s.downBuf.Len() > 0 || (s.downClosed && s.requestOutstanding)
	s.log.Debug("detached from tunnel; upstream retained", "wake", wake)
	s.mu.Unlock()
	s.cond.Broadcast()
	if wake {
		s.b.wake(s.actorID, "undelivered data on detach")
	}
}

// dialUpstream connects to the destination, flushes any bytes that arrived
// before the connection was ready, and starts the upstream reader.
func (s *session) dialUpstream(ctx context.Context, timeout time.Duration) {
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", s.target)
	if err != nil {
		s.log.Warn("upstream dial failed", "err", err)
		s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeClose, SessionID: s.sessionID, Dir: tunnel.DirDown, Reason: "dial failed: " + err.Error()})
		s.destroy()
		return
	}

	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.upstream = conn
	s.connected = true
	pre := s.preConn
	s.preConn = nil
	s.armWatchdogLocked()
	s.mu.Unlock()
	s.log.Info("upstream connected", "bufferedBytes", len(pre))

	// Flush pre-connect request bytes in order.
	if len(pre) > 0 {
		if _, err := conn.Write(pre); err != nil {
			s.closeUpstream(err)
			return
		}
		s.mu.Lock()
		s.upstreamWritten += uint64(len(pre))
		s.requestOutstanding = true
		written := s.upstreamWritten
		s.mu.Unlock()
		s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeAck, SessionID: s.sessionID, Offset: written})
	}
	go s.readUpstreamLoop()
}

// readUpstreamLoop reads the destination forever, feeding produceDownstream.
// It keeps reading even while the session is detached (actor suspended) —
// that is what lets the broker absorb the response during a suspend — until
// the downBuf fills, at which point backpressure (produceDownstream
// blocking) stops it and TCP flow control pushes back on the server.
func (s *session) readUpstreamLoop() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.upstream.Read(buf)
		if n > 0 {
			if perr := s.produceDownstream(buf[:n]); perr != nil {
				s.log.Warn("produceDownstream failed", "err", perr)
				s.destroy()
				return
			}
		}
		if err != nil {
			s.closeUpstream(err)
			return
		}
	}
}

// produceDownstream appends upstream→agent bytes to downBuf and forwards or
// buffers them per the current bind state. Blocks (backpressure) if downBuf
// is full until acks free space.
func (s *session) produceDownstream(p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(p) > 0 {
		if s.dead {
			return errors.New("session dead")
		}
		free := s.downBuf.Free()
		if free == 0 {
			// Backpressure: wait for an ack (or death) to free space.
			s.cond.Wait()
			continue
		}
		take := len(p)
		if take > free {
			take = free
		}
		chunk := p[:take]
		startOffset := s.downBuf.End()
		if err := s.downBuf.Append(chunk); err != nil {
			return err
		}

		// Try to forward if attached, but never trust write success as proof
		// of delivery: the bytes stay in downBuf until the sidecar ACKs, and
		// the wake decision below is based on attach state, not the write.
		if s.tconn != nil {
			f := tunnel.Frame{Type: tunnel.TypeData, SessionID: s.sessionID, Offset: startOffset, Payload: chunk}
			if err := s.tconn.write(f); err != nil {
				s.tconn = nil
			}
		}
		if s.tconn == nil {
			// Detached: response data arrived for a suspended actor. Wake it
			// so it re-attaches and drains downBuf. (detach() also wakes as a
			// backstop for the race where the tunnel dies during this write.)
			s.b.wake(s.actorID, "downstream data while detached")
		}
		p = p[take:]
	}
	return nil
}

// onData handles agent→upstream bytes from the sidecar. Offsets before
// upstreamWritten are replayed duplicates (post-reconnect) and skipped.
func (s *session) onData(offset uint64, payload []byte) {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return
	}
	// Before the upstream dial completes, buffer request bytes in order. The
	// "accepted" offset is what we've written plus what we've queued.
	if !s.connected {
		accepted := s.upstreamWritten + uint64(len(s.preConn))
		if offset < accepted {
			skip := accepted - offset
			if skip >= uint64(len(payload)) {
				s.mu.Unlock()
				return
			}
			payload = payload[skip:]
		} else if offset > accepted {
			s.log.Warn("out-of-order pre-connect data", "got", offset, "want", accepted)
			s.mu.Unlock()
			return
		}
		s.preConn = append(s.preConn, payload...)
		s.mu.Unlock()
		return
	}
	up := s.upstream
	// Drop the already-written prefix (replayed duplicates after a reconnect).
	if offset < s.upstreamWritten {
		skip := s.upstreamWritten - offset
		if skip >= uint64(len(payload)) {
			s.mu.Unlock()
			return
		}
		payload = payload[skip:]
		offset = s.upstreamWritten
	}
	if offset > s.upstreamWritten {
		// Gap — should not happen with in-order tunnel delivery; ignore
		// rather than corrupt the stream.
		s.log.Warn("out-of-order upstream data", "got", offset, "want", s.upstreamWritten)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	if _, err := up.Write(payload); err != nil {
		s.log.Warn("upstream write failed", "err", err)
		s.closeUpstream(err)
		return
	}

	s.mu.Lock()
	s.upstreamWritten += uint64(len(payload))
	written := s.upstreamWritten
	s.requestOutstanding = true // a request (or part of one) is now in flight
	s.armWatchdogLocked()
	s.mu.Unlock()

	// Ack so the sidecar can trim its upstream replay buffer.
	s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeAck, SessionID: s.sessionID, Offset: written})
}

// onDownstreamAck trims downBuf to the sidecar's delivered offset and wakes
// any producer blocked on backpressure. Once downBuf drains to empty, the
// outstanding request is considered fully answered, so a subsequent idle
// keep-alive close will not wake the actor.
func (s *session) onDownstreamAck(offset uint64) {
	s.mu.Lock()
	s.downBuf.TrimTo(offset)
	if s.downBuf.Len() == 0 {
		s.requestOutstanding = false
	}
	s.mu.Unlock()
	s.cond.Broadcast()
}

// onSidecarAttach re-binds after a reconnect: replay downstream bytes the
// sidecar has not yet delivered, and tell it how many upstream bytes we have
// already written so it can replay the rest.
func (s *session) onSidecarAttach(deliveredOffset uint64) {
	s.mu.Lock()
	written := s.upstreamWritten
	replay, err := s.downBuf.From(deliveredOffset)
	// Copy under lock; the buffer slice aliases downBuf.
	var replayCopy []byte
	if err == nil && len(replay) > 0 {
		replayCopy = make([]byte, len(replay))
		copy(replayCopy, replay)
	}
	downClosed := s.downClosed && s.downBuf.End() == deliveredOffset+uint64(len(replay))
	s.mu.Unlock()

	if err != nil {
		s.log.Warn("ATTACH replay failed", "err", err)
	}

	s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeAttachOK, SessionID: s.sessionID, Offset: written})
	if len(replayCopy) > 0 {
		s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeData, SessionID: s.sessionID, Offset: deliveredOffset, Payload: replayCopy})
		s.log.Info("replayed downstream after attach", "from", deliveredOffset, "bytes", len(replayCopy))
	}
	if downClosed {
		s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeClose, SessionID: s.sessionID, Dir: tunnel.DirDown, Reason: "upstream EOF"})
	}
}

// onSidecarClose handles a CLOSE from the sidecar (agent closed its side).
func (s *session) onSidecarClose(dir uint8, reason string) {
	if dir != tunnel.DirUp {
		return
	}
	s.mu.Lock()
	s.upClosed = true
	up := s.upstream
	s.mu.Unlock()
	if tc, ok := up.(interface{ CloseWrite() error }); ok {
		_ = tc.CloseWrite()
	}
	s.log.Info("agent closed upstream half", "reason", reason)
	s.maybeDestroy()
}

// closeUpstream handles upstream EOF/error: mark down-closed, forward or
// buffer a CLOSE, and wake if pending+detached.
func (s *session) closeUpstream(cause error) {
	s.mu.Lock()
	if s.downClosed {
		s.mu.Unlock()
		return
	}
	s.downClosed = true
	pending := s.requestOutstanding
	bound := s.tconn != nil
	s.mu.Unlock()

	reason := "upstream EOF"
	if cause != nil && !errors.Is(cause, io.EOF) {
		reason = cause.Error()
	}
	s.log.Info("upstream closed", "reason", reason, "pending", pending, "bound", bound)

	if bound {
		s.sendToSidecar(tunnel.Frame{Type: tunnel.TypeClose, SessionID: s.sessionID, Dir: tunnel.DirDown, Reason: reason})
	} else if pending {
		// Detached + pending: agent is waiting; it must see the close/error
		// and retry. Wake so it re-attaches and drains the CLOSE.
		s.b.wake(s.actorID, "upstream close on pending session")
	}
	// If detached + quiescent (idle keep-alive reaped), do nothing: the
	// CLOSE is delivered on the next attach, no spurious wake.
	s.maybeDestroy()
}

// sendToSidecar writes a frame to the bound tunnel, if any. Frames produced
// while detached are dropped on the floor here but their effect is preserved
// in buffers/flags and re-derived on ATTACH.
func (s *session) sendToSidecar(f tunnel.Frame) {
	s.mu.Lock()
	tc := s.tconn
	s.mu.Unlock()
	if tc == nil {
		return
	}
	if err := tc.write(f); err != nil {
		s.detach(tc)
	}
}

// maybeDestroy tears the session down once both halves are closed and the
// downstream buffer has drained (or is unrecoverable).
func (s *session) maybeDestroy() {
	s.mu.Lock()
	done := s.upClosed && s.downClosed && s.downBuf.Len() == 0
	s.mu.Unlock()
	if done {
		s.destroy()
	}
}

// destroy closes the upstream, removes the session, and unblocks waiters.
func (s *session) destroy() {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return
	}
	s.dead = true
	if s.upstream != nil {
		_ = s.upstream.Close()
	}
	if s.watchdog != nil {
		s.watchdog.Stop()
	}
	s.mu.Unlock()
	s.cond.Broadcast()
	s.b.removeSession(s.actorID, s.sessionID)
	s.log.Info("session destroyed")
}

// armWatchdogLocked (re)starts the max-suspend backstop. Any pending stream
// held longer than the watchdog resumes the actor regardless of the wake
// policy, so no bug can strand an actor with a request in flight. Caller
// holds s.mu.
func (s *session) armWatchdogLocked() {
	if s.watchdog != nil {
		s.watchdog.Stop()
	}
	d := s.b.cfg.MaxSuspendWatchdog
	s.watchdog = time.AfterFunc(d, func() {
		s.mu.Lock()
		pending := s.requestOutstanding
		detached := s.tconn == nil
		dead := s.dead
		s.mu.Unlock()
		if !dead && pending && detached {
			s.b.wake(s.actorID, "max-suspend watchdog")
		}
	})
}

func (s *session) info(k sessionKey) SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SessionInfo{
		ActorID:       k.actorID,
		SessionID:     k.sessionID,
		Target:        s.target,
		Attached:      s.tconn != nil,
		Pending:       s.requestOutstanding,
		DownBufBytes:  s.downBuf.Len(),
		UpstreamAlive: s.upstream != nil && !s.dead,
	}
}
