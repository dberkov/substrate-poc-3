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
	"log/slog"
	"sync"

	"github.com/dberkov/substrate-poc-3/internal/tunnel"
)

// tunnelConn is one live tunnel connection and the set of sessions
// currently bound to it. When the connection drops (actor suspend), every
// bound session is detached — its upstream stays open and its buffers stay
// intact, waiting for the next ATTACH to re-bind it to a fresh tunnelConn.
type tunnelConn struct {
	tc  *tunnel.Conn
	log *slog.Logger

	mu       sync.Mutex
	sessions map[uint64]*session
}

func (t *tunnelConn) register(s *session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sessions == nil {
		t.sessions = make(map[uint64]*session)
	}
	t.sessions[s.sessionID] = s
}

// detachAll unbinds every session bound to this tunnel connection. Called
// when the connection's frame loop exits.
func (t *tunnelConn) detachAll() {
	t.mu.Lock()
	sessions := make([]*session, 0, len(t.sessions))
	for _, s := range t.sessions {
		sessions = append(sessions, s)
	}
	t.sessions = nil
	t.mu.Unlock()

	for _, s := range sessions {
		s.detach(t)
	}
}

// write sends a frame on this tunnel connection. A write error means the
// connection is dying; the frame loop will observe the same and detach.
func (t *tunnelConn) write(f tunnel.Frame) error {
	return t.tc.WriteFrame(f)
}
