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

// Package sidecar is the in-actor half of the resumable egress tunnel: an
// HTTP(S) proxy the agent points at via HTTP(S)_PROXY, a tunnel client that
// multiplexes agent connections to the egress-broker and re-attaches them
// after every suspend, and the suspend poller (activity.go).
package sidecar

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/dberkov/substrate-poc-3/internal/tunnel"
)

// Client maintains a single tunnel connection to the broker on behalf of
// one actor, reconnecting and re-attaching all live sessions after every
// suspend. Sessions (agent connections) are created by the proxy and
// outlive tunnel connections.
type Client struct {
	// actorID reads the actor identity ("atespace/name") FRESH on every call.
	// It must not be cached: the sidecar's memory is frozen into the golden
	// snapshot, so a value read once at startup would be the golden actor's
	// name on every hydrated actor. Reading the bind-mounted /run/ate/actor-id
	// at use-time yields the real actor's name after restore.
	actorID       func() string
	brokerAddr    string
	sessionBuffer int
	pingInterval  time.Duration
	pongTimeout   time.Duration
	dialBackoff   time.Duration
	log           *slog.Logger

	nextID atomic.Uint64

	mu       sync.Mutex
	conn     *tunnel.Conn // current tunnel connection, or nil while reconnecting
	sessions map[uint64]*clientSession

	// pong tracking
	pongMu   sync.Mutex
	lastPong time.Time
}

// ClientConfig configures a Client.
type ClientConfig struct {
	// ActorID returns the actor identity ("atespace/name"), read fresh on
	// every call (see Client.actorID).
	ActorID       func() string
	BrokerAddr    string
	SessionBuffer int
	PingInterval  time.Duration
	PongTimeout   time.Duration
	DialBackoff   time.Duration
	Logger        *slog.Logger
}

func NewClient(cfg ClientConfig) *Client {
	if cfg.SessionBuffer == 0 {
		cfg.SessionBuffer = 4 * 1024 * 1024
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = time.Second
	}
	if cfg.PongTimeout == 0 {
		cfg.PongTimeout = 3 * time.Second
	}
	if cfg.DialBackoff == 0 {
		cfg.DialBackoff = 500 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Client{
		actorID:       cfg.ActorID,
		brokerAddr:    cfg.BrokerAddr,
		sessionBuffer: cfg.SessionBuffer,
		pingInterval:  cfg.PingInterval,
		pongTimeout:   cfg.PongTimeout,
		dialBackoff:   cfg.DialBackoff,
		log:           cfg.Logger,
		sessions:      make(map[uint64]*clientSession),
	}
}

// Run maintains the tunnel connection until ctx is cancelled: connect,
// HELLO, re-attach live sessions, pump frames, and on any failure reconnect
// after a backoff. This is the loop that heals the tunnel after a suspend.
func (c *Client) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.connectOnce(ctx); err != nil {
			c.log.Info("tunnel down; reconnecting", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.dialBackoff):
		}
	}
}

func (c *Client) connectOnce(ctx context.Context) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", c.brokerAddr)
	if err != nil {
		return err
	}
	tc := tunnel.NewConn(raw)
	// Read the actor identity FRESH here, not at construction: after a
	// restore from the golden snapshot the correct name is only available
	// from the bind-mounted file, and this is the first thing that runs on
	// each (re)connect after resume.
	actorID := c.actorID()
	if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeHello, ActorID: actorID}); err != nil {
		_ = tc.Close()
		return err
	}

	c.mu.Lock()
	c.conn = tc
	live := make([]*clientSession, 0, len(c.sessions))
	for _, s := range c.sessions {
		live = append(live, s)
	}
	c.mu.Unlock()
	c.setLastPong(time.Now())
	c.log.Info("tunnel connected", "actor", actorID, "broker", c.brokerAddr, "reattach", len(live))

	// Re-attach every live session so the broker resumes their byte streams.
	for _, s := range live {
		_ = tc.WriteFrame(s.attachFrame())
	}

	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go c.pingLoop(pingCtx, tc)

	err = c.readLoop(tc)

	c.mu.Lock()
	if c.conn == tc {
		c.conn = nil
	}
	c.mu.Unlock()
	_ = tc.Close()
	return err
}

// readLoop dispatches inbound frames to sessions until the connection fails.
func (c *Client) readLoop(tc *tunnel.Conn) error {
	for {
		f, err := tc.ReadFrame()
		if err != nil {
			return err
		}
		switch f.Type {
		case tunnel.TypePong:
			c.setLastPong(time.Now())
		case tunnel.TypePing:
			_ = tc.WriteFrame(tunnel.Frame{Type: tunnel.TypePong, Nonce: f.Nonce})
		case tunnel.TypeAttachOK:
			if s := c.lookup(f.SessionID); s != nil {
				s.onAttachOK(f.Offset)
			}
		case tunnel.TypeData:
			if s := c.lookup(f.SessionID); s != nil {
				s.deliverDown(f.Offset, f.Payload)
			}
		case tunnel.TypeAck:
			if s := c.lookup(f.SessionID); s != nil {
				s.onUpstreamAck(f.Offset)
			}
		case tunnel.TypeClose:
			if s := c.lookup(f.SessionID); s != nil {
				s.onClose(f.Dir, f.Reason)
			}
		default:
			c.log.Warn("unexpected frame from broker", "frame", f.String())
		}
	}
}

// pingLoop sends PINGs and closes the connection if a PONG is overdue — the
// primary detector of a zombie tunnel socket after a restore.
func (c *Client) pingLoop(ctx context.Context, tc *tunnel.Conn) {
	var nonce uint64
	t := time.NewTicker(c.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if time.Since(c.getLastPong()) > c.pongTimeout {
				c.log.Info("pong overdue; assuming zombie tunnel, forcing reconnect")
				_ = tc.Close() // unblocks readLoop
				return
			}
			nonce++
			if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypePing, Nonce: nonce}); err != nil {
				_ = tc.Close()
				return
			}
		}
	}
}

// OpenSession creates a new tunnel session for an accepted agent connection
// and returns it. The caller pumps agent→upstream via WriteUp and closes it
// when the agent side ends.
func (c *Client) OpenSession(agent net.Conn, target string) *clientSession {
	id := c.nextID.Add(1)
	s := newClientSession(c, id, target, agent)
	c.mu.Lock()
	c.sessions[id] = s
	c.mu.Unlock()
	c.send(tunnel.Frame{Type: tunnel.TypeOpen, SessionID: id, Target: target})
	return s
}

// WriteUp is the session's upstream producer entry point (used by the proxy).
func (s *clientSession) WriteUp(p []byte) error { return s.writeUp(p) }

// CloseUp signals the agent finished sending.
func (s *clientSession) CloseUp(reason string) { s.closeUp(reason) }

// send writes a frame on the current tunnel connection if one is bound.
// Frames dropped while the tunnel is down are re-derived on reconnect
// (buffers + ATTACH), so a nil connection is not an error.
func (c *Client) send(f tunnel.Frame) {
	c.mu.Lock()
	tc := c.conn
	c.mu.Unlock()
	if tc == nil {
		return
	}
	_ = tc.WriteFrame(f)
}

func (c *Client) lookup(id uint64) *clientSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[id]
}

func (c *Client) removeSession(id uint64) {
	c.mu.Lock()
	delete(c.sessions, id)
	c.mu.Unlock()
}

func (c *Client) setLastPong(t time.Time) {
	c.pongMu.Lock()
	c.lastPong = t
	c.pongMu.Unlock()
}

func (c *Client) getLastPong() time.Time {
	c.pongMu.Lock()
	defer c.pongMu.Unlock()
	return c.lastPong
}
