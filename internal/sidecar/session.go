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

package sidecar

import (
	"log/slog"
	"net"
	"sync"

	"github.com/dberkov/substrate-poc-3/internal/tunnel"
)

// clientSession is the sidecar's half of one tunnel session: the agent-side
// connection (a checkpointed loopback conn that survives suspend) plus the
// upstream replay buffer and the delivered-downstream offset needed to
// resume after the tunnel reconnects.
type clientSession struct {
	c         *Client
	sessionID uint64
	target    string
	agent     net.Conn
	log       *slog.Logger

	mu    sync.Mutex
	cond  *sync.Cond
	upBuf *tunnel.ReplayBuffer // agent→upstream, produced here

	downDelivered uint64 // upstream→agent bytes written to the agent conn
	upClosed      bool
	downClosed    bool
	dead          bool
}

func newClientSession(c *Client, id uint64, target string, agent net.Conn) *clientSession {
	s := &clientSession{
		c:         c,
		sessionID: id,
		target:    target,
		agent:     agent,
		log:       c.log.With("sid", id, "target", target),
		upBuf:     tunnel.NewReplayBuffer(c.sessionBuffer),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// writeUp forwards agent→upstream bytes: retain them for replay and send a
// DATA frame if the tunnel is currently up. Blocks (backpressure) when the
// replay buffer is full until a broker ACK trims it.
func (s *clientSession) writeUp(p []byte) error {
	s.mu.Lock()
	for len(p) > 0 {
		if s.dead {
			s.mu.Unlock()
			return net.ErrClosed
		}
		free := s.upBuf.Free()
		if free == 0 {
			s.cond.Wait()
			continue
		}
		take := len(p)
		if take > free {
			take = free
		}
		chunk := p[:take]
		offset := s.upBuf.End()
		if err := s.upBuf.Append(chunk); err != nil {
			s.mu.Unlock()
			return err
		}
		s.mu.Unlock()
		s.c.send(tunnel.Frame{Type: tunnel.TypeData, SessionID: s.sessionID, Offset: offset, Payload: chunk})
		s.mu.Lock()
		p = p[take:]
	}
	s.mu.Unlock()
	return nil
}

// onUpstreamAck trims the upstream replay buffer to the broker's confirmed
// offset and releases any writer blocked on backpressure.
func (s *clientSession) onUpstreamAck(offset uint64) {
	s.mu.Lock()
	s.upBuf.TrimTo(offset)
	s.mu.Unlock()
	s.cond.Broadcast()
}

// deliverDown writes upstream→agent bytes to the agent connection and acks
// the broker. Duplicate prefixes (replayed after a reconnect) are skipped.
func (s *clientSession) deliverDown(offset uint64, payload []byte) {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return
	}
	if offset < s.downDelivered {
		skip := s.downDelivered - offset
		if skip >= uint64(len(payload)) {
			s.mu.Unlock()
			return
		}
		payload = payload[skip:]
		offset = s.downDelivered
	}
	if offset > s.downDelivered {
		s.log.Warn("out-of-order downstream", "got", offset, "want", s.downDelivered)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	if _, err := s.agent.Write(payload); err != nil {
		s.log.Warn("agent write failed", "err", err)
		s.closeAll()
		return
	}
	s.mu.Lock()
	s.downDelivered += uint64(len(payload))
	delivered := s.downDelivered
	s.mu.Unlock()
	s.c.send(tunnel.Frame{Type: tunnel.TypeAck, SessionID: s.sessionID, Offset: delivered})
}

// attachFrame returns the ATTACH frame describing this session's resume
// point for a freshly reconnected tunnel.
func (s *clientSession) attachFrame() tunnel.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return tunnel.Frame{Type: tunnel.TypeAttach, SessionID: s.sessionID, Offset: s.downDelivered, Target: s.target}
}

// onAttachOK replays upstream bytes the broker has not yet written, after a
// reconnect. offset is the broker's upstreamWritten.
func (s *clientSession) onAttachOK(offset uint64) {
	s.mu.Lock()
	s.upBuf.TrimTo(offset)
	replay, err := s.upBuf.From(offset)
	var cp []byte
	if err == nil && len(replay) > 0 {
		cp = make([]byte, len(replay))
		copy(cp, replay)
	}
	s.mu.Unlock()
	if err != nil {
		s.log.Warn("attach replay failed", "err", err)
		return
	}
	if len(cp) > 0 {
		s.c.send(tunnel.Frame{Type: tunnel.TypeData, SessionID: s.sessionID, Offset: offset, Payload: cp})
		s.log.Info("replayed upstream after reconnect", "from", offset, "bytes", len(cp))
	}
}

// onClose handles a CLOSE from the broker.
func (s *clientSession) onClose(dir uint8, reason string) {
	if dir == tunnel.DirDown {
		s.mu.Lock()
		s.downClosed = true
		s.mu.Unlock()
		// Half-close the agent's read side so it observes upstream EOF.
		if cw, ok := s.agent.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		} else {
			s.closeAll()
		}
		s.log.Info("broker closed downstream", "reason", reason)
	}
}

// closeUp signals that the agent finished sending (its read side hit EOF).
func (s *clientSession) closeUp(reason string) {
	s.mu.Lock()
	if s.upClosed {
		s.mu.Unlock()
		return
	}
	s.upClosed = true
	s.mu.Unlock()
	s.c.send(tunnel.Frame{Type: tunnel.TypeClose, SessionID: s.sessionID, Dir: tunnel.DirUp, Reason: reason})
}

// closeAll tears the session down and closes the agent connection.
func (s *clientSession) closeAll() {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return
	}
	s.dead = true
	s.mu.Unlock()
	_ = s.agent.Close()
	s.cond.Broadcast()
	s.c.removeSession(s.sessionID)
	s.log.Debug("session closed")
}
