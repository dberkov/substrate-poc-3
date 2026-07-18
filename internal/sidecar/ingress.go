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
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Reply-to protocol headers. These must match the ingress-broker's, and
// must NOT be "X-Request-Id"/"x-request-id" — that collides with Envoy's
// reserved tracing header, which atenet mutates in transit.
const (
	headerReplyTo   = "X-Poc-Reply-To"
	headerRequestID = "X-Poc-Request-Id"
	headerStatus    = "X-Poc-Orig-Status"
	headerCT        = "X-Poc-Orig-Content-Type"
)

// Ingress is the in-actor half of the phase-3 reply-to ingress (DESIGN.md
// phase 3). atenet routes client requests to the actor's :80, where this
// listens. For each request it:
//
//  1. reads the request and the ingress-broker's X-Reply-To / X-Request-Id
//     headers, and acks 202 to atenet immediately (freeing that inbound
//     connection, which won't survive a suspend anyway);
//  2. forwards the request to the agent over loopback (:8080), which DOES
//     survive suspend — the agent may suspend/resume during its egress work
//     while this call is in flight;
//  3. delivers the agent's response OUTBOUND to X-Reply-To (the survivable
//     direction), with retry, so the ingress-broker can hand it to the
//     still-parked client.
//
// The agent and client stay unaware of suspend/resume; only the survivable
// outbound leg carries the response.
type Ingress struct {
	agentBaseURL string       // e.g. http://127.0.0.1:8080
	agentClient  *http.Client // loopback; no timeout (survives suspend)
	replyClient  *http.Client // outbound to ingress-broker; keep-alives off
	log          *slog.Logger

	// In-flight request tracking, the ground-truth idle signal for the
	// suspend poller: the sidecar owns the whole ingress request→reply cycle,
	// so inFlight==0 means the actor has no client work outstanding.
	mu        sync.Mutex
	inFlight  int
	idleSince time.Time // when inFlight last hit 0; zero while a request is in flight
}

// NewIngress builds an Ingress that forwards to agentAddr (host:port).
func NewIngress(agentAddr string, log *slog.Logger) *Ingress {
	if log == nil {
		log = slog.Default()
	}
	return &Ingress{
		agentBaseURL: "http://" + agentAddr,
		// No timeout: the agent call spans the actor's suspend/resume over a
		// loopback connection that survives it.
		agentClient: &http.Client{},
		// Keep-alives off + used with retry: the reply is delivered right
		// after a resume, when a pooled connection from a prior turn is dead
		// (same rationale as the agent's /notify in earlier phases).
		replyClient: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}},
		log:         log,
		idleSince:   time.Now(), // starts idle
	}
}

// IdleDuration reports how long there has been NO in-flight client request,
// or 0 while one is being processed. The suspend poller uses this as a
// precise, race-free idle signal (the sidecar owns request→reply, so it
// knows exactly when work is outstanding).
func (i *Ingress) IdleDuration() time.Duration {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.inFlight > 0 || i.idleSince.IsZero() {
		return 0
	}
	return time.Since(i.idleSince)
}

func (i *Ingress) enterRequest() {
	i.mu.Lock()
	i.inFlight++
	i.idleSince = time.Time{}
	i.mu.Unlock()
}

func (i *Ingress) exitRequest() {
	i.mu.Lock()
	if i.inFlight > 0 {
		i.inFlight--
	}
	if i.inFlight == 0 {
		i.idleSince = time.Now()
	}
	i.mu.Unlock()
}

// Serve accepts atenet-routed client connections until ctx is cancelled.
func (i *Ingress) Serve(ctx context.Context, ln net.Listener) error {
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
		go i.handle(conn)
	}
}

func (i *Ingress) handle(conn net.Conn) {
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		if err != io.EOF {
			i.log.Debug("ingress read request", "err", err)
		}
		_ = conn.Close()
		return
	}
	replyTo := req.Header.Get(headerReplyTo)
	reqID := req.Header.Get(headerRequestID)

	// Health/readiness probes (or any request without a reply-to) are handled
	// inline rather than via the reply-to path.
	if replyTo == "" || reqID == "" {
		i.proxyInline(conn, req)
		return
	}

	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		_ = conn.Close()
		return
	}
	// Ack atenet immediately and release its (non-survivable) connection; the
	// real response is delivered out-of-band to the ingress-broker.
	_, _ = conn.Write([]byte("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n"))
	_ = conn.Close()

	i.enterRequest()
	go i.process(req, body, replyTo, reqID)
}

// process forwards the request to the agent (loopback, survives suspend) and
// delivers the response to the ingress-broker's reply endpoint (outbound).
func (i *Ingress) process(req *http.Request, body []byte, replyTo, reqID string) {
	defer i.exitRequest()
	url := i.agentBaseURL + req.URL.RequestURI()
	areq, err := http.NewRequest(req.Method, url, bytes.NewReader(body))
	if err != nil {
		i.log.Warn("ingress build agent request", "err", err)
		return
	}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		areq.Header.Set("Content-Type", ct)
	}

	resp, err := i.agentClient.Do(areq)
	if err != nil {
		i.log.Warn("ingress agent call failed", "reqID", reqID, "err", err)
		i.deliver(replyTo, reqID, http.StatusBadGateway, "text/plain", []byte("agent call failed: "+err.Error()))
		return
	}
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	i.deliver(replyTo, reqID, resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
}

// deliver posts the response outbound to the ingress-broker, retrying — it
// fires right after a resume, when a stale pooled connection would fail.
func (i *Ingress) deliver(replyTo, reqID string, status int, contentType string, body []byte) {
	url := "http://" + replyTo + "/reply"
	for attempt := 1; attempt <= 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			i.log.Warn("ingress build reply", "err", err)
			return
		}
		req.Header.Set(headerRequestID, reqID)
		req.Header.Set(headerStatus, strconv.Itoa(status))
		req.Header.Set(headerCT, contentType)
		resp, err := i.replyClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			i.log.Info("ingress reply delivered", "reqID", reqID, "status", status, "attempt", attempt)
			return
		}
		i.log.Info("ingress reply attempt failed", "reqID", reqID, "attempt", attempt, "err", err)
		time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
	}
	i.log.Warn("ingress reply gave up", "reqID", reqID)
}

// proxyInline handles probe/no-reply-to requests by forwarding to the agent
// and returning the response on the same connection (used for /readyz-style
// checks that don't need the reply-to path).
func (i *Ingress) proxyInline(conn net.Conn, req *http.Request) {
	defer conn.Close()
	body, _ := io.ReadAll(req.Body)
	_ = req.Body.Close()
	areq, err := http.NewRequest(req.Method, i.agentBaseURL+req.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		_, _ = conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	resp, err := i.agentClient.Do(areq)
	if err != nil {
		_, _ = conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	defer resp.Body.Close()
	_ = resp.Write(conn)
}
