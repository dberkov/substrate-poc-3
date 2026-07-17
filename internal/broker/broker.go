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

// Package broker is the egress-broker: the outside-the-actor half of the
// resumable tunnel (DESIGN.md §3, §7). It terminates tunnel connections
// from sidecars, dials each session's real destination, and — crucially —
// keeps the upstream connection open across actor suspends so the MCP
// server / LLM never observes a disconnect. It owns resume: when data or a
// close arrives on a suspended actor's pending session, it calls
// ResumeActor.
package broker

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/dberkov/substrate-poc-3/internal/ateapi"
	"github.com/dberkov/substrate-poc-3/internal/tunnel"
)

// Config configures a Broker.
type Config struct {
	// Lifecycle drives actor resume. Required.
	Lifecycle ateapi.Lifecycle
	// SessionBufferBytes caps each direction's replay buffer per session.
	SessionBufferBytes int
	// MaxSuspendWatchdog resumes any actor with a pending session held
	// longer than this, as a backstop against a wake-policy bug.
	MaxSuspendWatchdog time.Duration
	// DialTimeout bounds dialing a session's upstream destination.
	DialTimeout time.Duration
	// TunnelReadTimeout detaches a tunnel whose sidecar has gone silent
	// (suspended actors send no FIN/RST). Must exceed the sidecar's PING
	// interval. Default 4s.
	TunnelReadTimeout time.Duration
	// AllowTarget, if non-nil, gates which upstream host:port an OPEN may
	// dial. Returning false rejects the session. (Phase-4 policy lives
	// here; nil allows all.)
	AllowTarget func(target string) bool
	Logger      *slog.Logger
}

func (c *Config) withDefaults() {
	if c.SessionBufferBytes == 0 {
		c.SessionBufferBytes = 4 * 1024 * 1024
	}
	if c.MaxSuspendWatchdog == 0 {
		c.MaxSuspendWatchdog = 5 * time.Minute
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.TunnelReadTimeout == 0 {
		c.TunnelReadTimeout = 4 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Broker holds all live sessions keyed by (actorID, sessionID). Sessions
// outlive the tunnel connection that created them: a suspend drops the
// tunnel, but the session (and its upstream socket + buffers) stays until
// the stream truly ends or the watchdog reaps it.
type Broker struct {
	cfg     Config
	resumer *resumer
	log     *slog.Logger

	mu       sync.Mutex
	sessions map[sessionKey]*session
}

type sessionKey struct {
	actorID   string
	sessionID uint64
}

// New returns a Broker ready to Serve tunnel connections.
func New(cfg Config) *Broker {
	cfg.withDefaults()
	return &Broker{
		cfg:      cfg,
		resumer:  newResumer(cfg.Lifecycle, cfg.Logger),
		log:      cfg.Logger,
		sessions: make(map[sessionKey]*session),
	}
}

// Serve runs the tunnel-connection accept loop until ctx is cancelled.
func (b *Broker) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go b.serveTunnel(ctx, tunnel.NewConn(c))
	}
}

// serveTunnel runs one tunnel connection's frame loop. When it returns
// (suspend, network error, actor gone) the sessions it referenced are
// detached, NOT destroyed — a later ATTACH from the resumed actor re-binds
// them.
func (b *Broker) serveTunnel(ctx context.Context, tc *tunnel.Conn) {
	defer tc.Close()

	// The first frame must be HELLO.
	first, err := tc.ReadFrame()
	if err != nil {
		b.log.Debug("tunnel closed before HELLO", "err", err)
		return
	}
	if first.Type != tunnel.TypeHello || first.ActorID == "" {
		b.log.Warn("tunnel first frame not HELLO", "frame", first.String())
		return
	}
	actorID := first.ActorID
	log := b.log.With("actor", actorID, "tunnel", tc.RemoteAddr().String())
	log.Info("tunnel connected")

	tconn := &tunnelConn{tc: tc, log: log}
	defer tconn.detachAll()

	// The sidecar PINGs on an interval; silence beyond TunnelReadTimeout means
	// the actor was suspended (its frozen socket sends no FIN/RST), so treat
	// the read timeout as a prompt detach signal rather than blocking for the
	// kernel's TCP timeout.
	for {
		_ = tc.SetReadDeadline(time.Now().Add(b.cfg.TunnelReadTimeout))
		f, err := tc.ReadFrame()
		if err != nil {
			log.Info("tunnel disconnected (likely suspend); sessions retained", "err", err)
			return
		}
		b.handleFrame(ctx, actorID, tconn, f)
	}
}

func (b *Broker) handleFrame(ctx context.Context, actorID string, tconn *tunnelConn, f tunnel.Frame) {
	switch f.Type {
	case tunnel.TypePing:
		_ = tconn.tc.WriteFrame(tunnel.Frame{Type: tunnel.TypePong, Nonce: f.Nonce})

	case tunnel.TypeOpen:
		b.openSession(ctx, actorID, tconn, f)

	case tunnel.TypeAttach:
		b.attachSession(actorID, tconn, f)

	case tunnel.TypeData:
		if s := b.lookup(actorID, f.SessionID); s != nil {
			s.onData(f.Offset, f.Payload)
		}

	case tunnel.TypeAck:
		if s := b.lookup(actorID, f.SessionID); s != nil {
			s.onDownstreamAck(f.Offset)
		}

	case tunnel.TypeClose:
		if s := b.lookup(actorID, f.SessionID); s != nil {
			s.onSidecarClose(f.Dir, f.Reason)
		}

	default:
		b.log.Warn("unexpected frame from sidecar", "frame", f.String())
	}
}

func (b *Broker) openSession(ctx context.Context, actorID string, tconn *tunnelConn, f tunnel.Frame) {
	key := sessionKey{actorID, f.SessionID}
	if b.cfg.AllowTarget != nil && !b.cfg.AllowTarget(f.Target) {
		b.log.Warn("OPEN rejected by policy", "actor", actorID, "sid", f.SessionID, "target", f.Target)
		_ = tconn.tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeClose, SessionID: f.SessionID, Dir: tunnel.DirDown, Reason: "target not allowed"})
		return
	}

	b.mu.Lock()
	if _, exists := b.sessions[key]; exists {
		// Duplicate OPEN (e.g. sidecar retried after a lost OPEN). Treat as
		// attach-at-zero.
		b.mu.Unlock()
		b.attachSession(actorID, tconn, tunnel.Frame{SessionID: f.SessionID, Offset: 0, Target: f.Target})
		return
	}
	s := newSession(b, actorID, f.SessionID, f.Target)
	b.sessions[key] = s
	b.mu.Unlock()

	s.bind(tconn)
	// Dial off the read loop so a slow upstream doesn't stall PING responses
	// (which would make the sidecar reconnect spuriously). DATA that arrives
	// before the dial completes is buffered by the session.
	go s.dialUpstream(context.Background(), b.cfg.DialTimeout)
}

func (b *Broker) attachSession(actorID string, tconn *tunnelConn, f tunnel.Frame) {
	s := b.lookup(actorID, f.SessionID)
	if s == nil {
		// The session is gone (watchdog reaped it, or it fully closed). If
		// the sidecar has delivered nothing yet, recreate; otherwise the
		// stream is unrecoverable — tell the sidecar to close.
		if f.Offset == 0 && f.Target != "" {
			// The session's OPEN never landed — the common case is a resume:
			// the actor comes back and the agent issues its request before the
			// sidecar has finished re-dialing the tunnel, so OPEN is dropped
			// and only this ATTACH arrives. Recreate the session AND send
			// ATTACH_OK, so the sidecar replays the request bytes it buffered
			// (without ATTACH_OK the client waits forever and the request
			// never reaches upstream).
			b.log.Info("ATTACH for unknown session; recreating", "actor", actorID, "sid", f.SessionID)
			b.openSession(context.Background(), actorID, tconn, tunnel.Frame{Type: tunnel.TypeOpen, SessionID: f.SessionID, Target: f.Target})
			if s := b.lookup(actorID, f.SessionID); s != nil {
				s.onSidecarAttach(f.Offset)
			}
			return
		}
		b.log.Warn("ATTACH for unrecoverable session", "actor", actorID, "sid", f.SessionID, "delivered", f.Offset)
		_ = tconn.tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeClose, SessionID: f.SessionID, Dir: tunnel.DirDown, Reason: "session lost"})
		return
	}
	s.bind(tconn)
	s.onSidecarAttach(f.Offset)
}

func (b *Broker) lookup(actorID string, sessionID uint64) *session {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessions[sessionKey{actorID, sessionID}]
}

func (b *Broker) removeSession(actorID string, sessionID uint64) {
	b.mu.Lock()
	delete(b.sessions, sessionKey{actorID, sessionID})
	b.mu.Unlock()
}

// wake asks the resumer to bring actorID back. Called by a session when a
// wake condition fires on a detached, pending stream.
func (b *Broker) wake(actorID, reason string) {
	b.log.Info("waking actor", "actor", actorID, "reason", reason)
	b.resumer.resume(actorID)
}

// Sessions returns a snapshot for the debug endpoint.
func (b *Broker) Sessions() []SessionInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]SessionInfo, 0, len(b.sessions))
	for k, s := range b.sessions {
		out = append(out, s.info(k))
	}
	return out
}

// SessionInfo is a debug snapshot of one session.
type SessionInfo struct {
	ActorID       string `json:"actorId"`
	SessionID     uint64 `json:"sessionId"`
	Target        string `json:"target"`
	Attached      bool   `json:"attached"`
	Pending       bool   `json:"pending"`
	UpBufferBytes int    `json:"upBufferBytes"`
	DownBufBytes  int    `json:"downBufferBytes"`
	UpstreamAlive bool   `json:"upstreamAlive"`
}
