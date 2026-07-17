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
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
)

// Proxy is the forward proxy the agent points at via HTTP(S)_PROXY. It
// terminates the agent's proxy protocol and splices each connection to a
// resumable tunnel session:
//
//   - CONNECT host:port  (HTTPS, phase 2): reply 200, then splice raw bytes
//     both ways — the agent runs TLS end-to-end to the real server through
//     the tunnel, so the tunnel/broker never see plaintext.
//   - absolute-form http:// requests (phase 1): open a session to the target,
//     rewrite each request to origin-form onto the session's upstream stream,
//     and copy responses back verbatim.
//
// The agent, MCP server, and LLM are all unaware of the tunnel; only the
// destination host:port is extracted, never the payload.
type Proxy struct {
	client *Client
	log    *slog.Logger
}

func NewProxy(client *Client, log *slog.Logger) *Proxy {
	if log == nil {
		log = slog.Default()
	}
	return &Proxy{client: client, log: log}
}

// Serve accepts agent connections until ctx is cancelled.
func (p *Proxy) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go p.handle(conn)
	}
}

func (p *Proxy) handle(agent net.Conn) {
	br := bufio.NewReader(agent)
	req, err := http.ReadRequest(br)
	if err != nil {
		if err != io.EOF {
			p.log.Debug("read first request", "err", err)
		}
		_ = agent.Close()
		return
	}

	if req.Method == http.MethodConnect {
		p.handleConnect(agent, req)
		return
	}
	p.handleHTTP(agent, br, req)
}

// handleConnect implements the CONNECT tunnel (opaque byte splice).
func (p *Proxy) handleConnect(agent net.Conn, req *http.Request) {
	target := hostPort(req.Host, "443")
	if _, err := agent.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = agent.Close()
		return
	}
	s := p.client.OpenSession(agent, target)
	p.log.Info("CONNECT tunnel open", "target", target, "sid", s.sessionID)
	// Agent→upstream: raw copy of the (encrypted) byte stream.
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := agent.Read(buf)
			if n > 0 {
				if werr := s.WriteUp(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				s.CloseUp(closeReason(err))
				return
			}
		}
	}()
	// Downstream is delivered to `agent` by the tunnel read loop; nothing to
	// do here. The session is torn down when either half closes.
}

// handleHTTP proxies plain HTTP: one session per agent connection, targeted
// by the first request; each request is rewritten to origin-form and pushed
// onto the session's upstream stream. Responses flow back through the tunnel
// read loop's deliverDown.
func (p *Proxy) handleHTTP(agent net.Conn, br *bufio.Reader, first *http.Request) {
	target := hostPort(first.Host, "80")
	s := p.client.OpenSession(agent, target)
	p.log.Info("HTTP proxy session open", "target", target, "sid", s.sessionID)

	req := first
	for {
		reqTarget := hostPort(req.Host, "80")
		if reqTarget != target {
			// Keep-alive request to a different host: phase-1 limitation.
			// MCP/LLM clients keep one host per connection, so this is only
			// a safety log, not a correctness path for the demo.
			p.log.Warn("keep-alive host change not supported; closing session",
				"was", target, "now", reqTarget)
			s.CloseUp("host change")
			return
		}
		if err := writeOriginForm(s, req); err != nil {
			p.log.Warn("forward request failed", "err", err)
			s.CloseUp(closeReason(err))
			return
		}

		next, err := http.ReadRequest(br)
		if err != nil {
			s.CloseUp(closeReason(err))
			return
		}
		req = next
	}
}

// writeOriginForm serializes req in origin-form (path + Host header, no
// scheme/authority) onto the session's upstream stream, so an ordinary
// origin server accepts it.
func writeOriginForm(s *clientSession, req *http.Request) error {
	req.RequestURI = ""
	req.URL.Scheme = ""
	req.URL.Host = ""
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(req.Write(pw))
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			if werr := s.WriteUp(buf[:n]); werr != nil {
				_ = pr.CloseWithError(werr)
				return werr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func hostPort(host, defaultPort string) string {
	if host == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return net.JoinHostPort(host, defaultPort)
}

func closeReason(err error) string {
	if err == nil || err == io.EOF {
		return "eof"
	}
	if strings.Contains(err.Error(), "closed") {
		return "closed"
	}
	return fmt.Sprintf("err: %v", err)
}
