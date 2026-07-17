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

// Package e2e exercises the broker + sidecar tunnel against real sockets:
// the suspend/replay path (TestSuspendReplaysResponse) and the full
// HTTP-proxy happy path (TestHTTPProxyHappyPath).
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/dberkov/substrate-poc-3/internal/ateapi"
	"github.com/dberkov/substrate-poc-3/internal/broker"
	"github.com/dberkov/substrate-poc-3/internal/sidecar"
	"github.com/dberkov/substrate-poc-3/internal/tunnel"
)

// fakeLC records lifecycle calls and signals resumes on a channel.
type fakeLC struct {
	mu      sync.Mutex
	resumes []ateapi.Ref
	resumed chan ateapi.Ref
}

func newFakeLC() *fakeLC { return &fakeLC{resumed: make(chan ateapi.Ref, 16)} }

func (f *fakeLC) SuspendActor(context.Context, ateapi.Ref) error { return nil }
func (f *fakeLC) ResumeActor(_ context.Context, r ateapi.Ref) error {
	f.mu.Lock()
	f.resumes = append(f.resumes, r)
	f.mu.Unlock()
	select {
	case f.resumed <- r:
	default:
	}
	return nil
}
func (f *fakeLC) resumeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.resumes)
}

// startBroker spins a broker on an ephemeral port and returns its tunnel
// address.
func startBroker(t *testing.T, lc ateapi.Lifecycle) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := broker.New(broker.Config{Lifecycle: lc, MaxSuspendWatchdog: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Serve(ctx, ln)
	return ln.Addr().String()
}

// TestSuspendReplaysResponse drives the tunnel with raw frames so the
// suspend point is deterministic: the request reaches upstream, the tunnel
// is cut (suspend), upstream responds while detached (broker must wake), and
// a fresh ATTACH replays the buffered response byte-for-byte.
func TestSuspendReplaysResponse(t *testing.T) {
	// Upstream: read the request line, wait for the gate, then reply.
	gate := make(chan struct{})
	response := bytes.Repeat([]byte("RESPONSE-DATA-0123456789"), 500) // ~12KB
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { upstreamLn.Close() })
	go func() {
		c, err := upstreamLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 7)
		if _, err := io.ReadFull(c, buf); err != nil {
			return
		}
		<-gate
		_, _ = c.Write(response)
	}()

	lc := newFakeLC()
	brokerAddr := startBroker(t, lc)

	// Raw tunnel client, connection #1.
	dialTunnel := func() *tunnel.Conn {
		raw, err := net.Dial("tcp", brokerAddr)
		if err != nil {
			t.Fatal(err)
		}
		tc := tunnel.NewConn(raw)
		if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeHello, ActorID: "demo/actor-1"}); err != nil {
			t.Fatal(err)
		}
		return tc
	}

	tc := dialTunnel()
	const sid = 1
	if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeOpen, SessionID: sid, Target: upstreamLn.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	// Send the "request" upstream.
	if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeData, SessionID: sid, Offset: 0, Payload: []byte("REQUEST")}); err != nil {
		t.Fatal(err)
	}
	// Expect an ACK for the upstream write (offset 7).
	waitForFrame(t, tc, func(f tunnel.Frame) bool {
		return f.Type == tunnel.TypeAck && f.SessionID == sid && f.Offset == 7
	})

	// SUSPEND: cut the tunnel. The broker keeps the upstream open.
	_ = tc.Close()

	// Upstream now responds while the actor is "suspended" (detached).
	close(gate)

	// The broker must wake the actor: downstream data on a pending session.
	select {
	case r := <-lc.resumed:
		if r.Atespace != "demo" || r.Name != "actor-1" {
			t.Fatalf("resumed wrong actor: %v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("broker did not call ResumeActor after downstream on detached pending session")
	}

	// RESUME: reconnect and ATTACH from delivered offset 0.
	tc2 := dialTunnel()
	defer tc2.Close()
	if err := tc2.WriteFrame(tunnel.Frame{Type: tunnel.TypeAttach, SessionID: sid, Offset: 0, Target: upstreamLn.Addr().String()}); err != nil {
		t.Fatal(err)
	}

	// Collect downstream DATA until we have the whole response.
	got := readDownstream(t, tc2, sid, len(response))
	if !bytes.Equal(got, response) {
		t.Fatalf("replayed response mismatch: got %d bytes, want %d", len(got), len(response))
	}
}

// TestAttachWithoutOpenReplays covers the resume race: the session's OPEN was
// dropped (tunnel down while the actor re-dialed), so only an ATTACH arrives
// for a session the broker has never seen. The broker must recreate it AND
// send ATTACH_OK, so the sidecar replays the request it buffered — otherwise
// the request never reaches upstream and the run hangs.
func TestAttachWithoutOpenReplays(t *testing.T) {
	response := []byte("PONG-RESPONSE")
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { upstreamLn.Close() })
	gotReq := make(chan string, 1)
	go func() {
		c, err := upstreamLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 7)
		if _, err := io.ReadFull(c, buf); err != nil {
			return
		}
		gotReq <- string(buf)
		_, _ = c.Write(response)
	}()

	lc := newFakeLC()
	brokerAddr := startBroker(t, lc)

	raw, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatal(err)
	}
	tc := tunnel.NewConn(raw)
	defer tc.Close()
	if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeHello, ActorID: "demo/actor-attach"}); err != nil {
		t.Fatal(err)
	}
	const sid = 1
	// ATTACH with NO prior OPEN, delivered offset 0 — the dropped-OPEN case.
	if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeAttach, SessionID: sid, Offset: 0, Target: upstreamLn.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	// The broker must acknowledge the attach so a real client would replay.
	waitForFrame(t, tc, func(f tunnel.Frame) bool {
		return f.Type == tunnel.TypeAttachOK && f.SessionID == sid
	})
	// Now (as the client's replay would) send the buffered request.
	if err := tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeData, SessionID: sid, Offset: 0, Payload: []byte("REQUEST")}); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-gotReq:
		if got != "REQUEST" {
			t.Fatalf("upstream got %q, want REQUEST", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never received the replayed request (ATTACH_OK/replay broken)")
	}
	got := readDownstream(t, tc, sid, len(response))
	if !bytes.Equal(got, response) {
		t.Fatalf("response mismatch: got %q want %q", got, response)
	}
}

// TestHTTPProxyHappyPath runs a real HTTP request through the sidecar proxy
// + tunnel + broker to a real HTTP upstream, verifying the origin-form
// rewrite and byte plumbing deliver a correct response.
func TestHTTPProxyHappyPath(t *testing.T) {
	want := bytes.Repeat([]byte("A"), 100000)
	upstream := &http.Server{}
	upLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		if r.Host == "" {
			t.Error("upstream received empty Host (origin-form rewrite failed)")
		}
		w.Header().Set("X-Test", "ok")
		_, _ = w.Write(want)
	})
	upstream.Handler = mux
	go upstream.Serve(upLn)
	t.Cleanup(func() { upstream.Close() })

	lc := newFakeLC()
	brokerAddr := startBroker(t, lc)

	client := sidecar.NewClient(sidecar.ClientConfig{
		ActorID:      func() string { return "demo/actor-http" },
		BrokerAddr:   brokerAddr,
		PingInterval: 200 * time.Millisecond,
		PongTimeout:  time.Second,
		DialBackoff:  100 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go client.Run(ctx)

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := sidecar.NewProxy(client, nil)
	go proxy.Serve(ctx, proxyLn)

	// Give the tunnel a moment to connect.
	time.Sleep(200 * time.Millisecond)

	proxyURL, _ := url.Parse("http://" + proxyLn.Addr().String())
	httpClient := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
		Timeout:   10 * time.Second,
	}
	resp, err := httpClient.Get("http://" + upLn.Addr().String() + "/echo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("X-Test") != "ok" {
		t.Fatalf("missing response header; status=%d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("body mismatch: got %d bytes want %d", len(body), len(want))
	}
}

func waitForFrame(t *testing.T, tc *tunnel.Conn, pred func(tunnel.Frame) bool) {
	t.Helper()
	_ = tc.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer tc.SetReadDeadline(time.Time{})
	for {
		f, err := tc.ReadFrame()
		if err != nil {
			t.Fatalf("waiting for frame: %v", err)
		}
		if pred(f) {
			return
		}
	}
}

func readDownstream(t *testing.T, tc *tunnel.Conn, sid uint64, want int) []byte {
	t.Helper()
	_ = tc.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer tc.SetReadDeadline(time.Time{})
	var got []byte
	for len(got) < want {
		f, err := tc.ReadFrame()
		if err != nil {
			t.Fatalf("reading downstream (have %d/%d): %v", len(got), want, err)
		}
		if f.Type == tunnel.TypeData && f.SessionID == sid {
			got = append(got, f.Payload...)
			// Ack so the broker trims (and would unblock backpressure).
			_ = tc.WriteFrame(tunnel.Frame{Type: tunnel.TypeAck, SessionID: sid, Offset: uint64(len(got))})
		}
	}
	return got
}

var _ = fmt.Sprintf
