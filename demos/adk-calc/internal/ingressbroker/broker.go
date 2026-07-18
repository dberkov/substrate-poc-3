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

// Package ingressbroker is the client-transparent ingress relay
// (DESIGN.md §8). It makes the client unaware of suspend/resume using a
// reply-to callback rather than an app-specific park/notify/dedup dance:
//
//   - /run (client-facing): holds the client's connection, forwards the
//     request to the actor THROUGH atenet (which wakes it), tagged with
//     X-Reply-To (this instance's directly-reachable address) and a unique
//     X-Request-Id, then waits for the reply.
//   - /reply (sidecar-facing): the actor's egress-sidecar delivers the
//     response OUTBOUND here (the direction that survives suspend); we match
//     X-Request-Id back to the held client connection and write it.
//
// Because the response is addressed to a specific instance (X-Reply-To =
// pod IP), N broker replicas behind a plain L4 LB need no rendezvous /
// consistent-hashing: a client hits any instance, and the reply comes back
// to that exact instance. The broker calls NO substrate API — atenet does
// the resume.
package ingressbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const actorHostSuffix = ".actors.resources.substrate.ate.dev"

// Header names for the reply-to protocol. NOT "X-Request-Id": that collides
// with Envoy's reserved x-request-id, which atenet mutates (it packs a
// trace-sampling decision into a UUID nibble) — corrupting our id in transit.
const (
	headerReplyTo   = "X-Poc-Reply-To"
	headerRequestID = "X-Poc-Request-Id"
	headerStatus    = "X-Poc-Orig-Status"
	headerCT        = "X-Poc-Orig-Content-Type"
)

// Config configures a Broker.
type Config struct {
	// AtenetAddr is the atenet router that fronts actors (host:port).
	AtenetAddr string
	// Atespace the actors live in (to build the routing Host).
	Atespace string
	// ReplyAddr is THIS instance's address the sidecar posts replies to
	// (e.g. "10.1.2.3:9090"). Must be directly reachable from actors.
	ReplyAddr string
	// RequestTimeout caps how long a parked client request waits for its
	// reply before giving up.
	RequestTimeout time.Duration
}

type Broker struct {
	cfg     Config
	fwd     *http.Client // forwards to atenet (wake + deliver)
	mu      sync.Mutex
	pending map[string]chan replyMsg
}

type replyMsg struct {
	status      int
	contentType string
	body        []byte
}

type runReq struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
}

func New(cfg Config) *Broker {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 10 * time.Minute
	}
	return &Broker{
		cfg:     cfg,
		fwd:     &http.Client{Timeout: 60 * time.Second},
		pending: make(map[string]chan replyMsg),
	}
}

// HandleRun is the client-facing endpoint. The client is a plain HTTP
// caller: it POSTs its request and blocks for the response, unaware that an
// actor is being suspended/resumed underneath.
func (b *Broker) HandleRun(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req runReq
	if err := json.Unmarshal(body, &req); err != nil || req.SessionID == "" {
		http.Error(w, "invalid JSON or missing sessionId", http.StatusBadRequest)
		return
	}

	reqID := uuid.NewString()
	ch := make(chan replyMsg, 1)
	b.register(reqID, ch)
	defer b.unregister(reqID)

	log.Printf("ingress: session=%s reqID=%s forwarding via atenet", req.SessionID, reqID)
	if err := b.forwardToActor(r.Context(), req.SessionID, body, reqID); err != nil {
		log.Printf("ingress: session=%s reqID=%s forward failed: %v", req.SessionID, reqID, err)
		http.Error(w, "forward to actor failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	select {
	case reply := <-ch:
		if reply.contentType != "" {
			w.Header().Set("Content-Type", reply.contentType)
		}
		w.WriteHeader(reply.status)
		_, _ = w.Write(reply.body)
		log.Printf("ingress: session=%s reqID=%s delivered %d bytes status=%d", req.SessionID, reqID, len(reply.body), reply.status)
	case <-r.Context().Done():
		log.Printf("ingress: session=%s reqID=%s client cancelled", req.SessionID, reqID)
	case <-time.After(b.cfg.RequestTimeout):
		log.Printf("ingress: session=%s reqID=%s timed out waiting for reply", req.SessionID, reqID)
		http.Error(w, "timed out waiting for actor response", http.StatusGatewayTimeout)
	}
}

// forwardToActor delivers the request to the actor via atenet. atenet wakes
// the actor (its ext_proc calls ResumeActor) and routes to the sidecar's
// :80, which acks 202 and processes asynchronously — so this returns quickly
// once the actor is up; the real response arrives later on /reply. Retries a
// transient wake race.
func (b *Broker) forwardToActor(ctx context.Context, sessionID string, body []byte, reqID string) error {
	url := fmt.Sprintf("http://%s/run", b.cfg.AtenetAddr)
	host := sessionID + "." + b.cfg.Atespace + actorHostSuffix

	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Host = host
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(headerReplyTo, b.cfg.ReplyAddr)
		req.Header.Set(headerRequestID, reqID)

		resp, err := b.fwd.Do(req)
		if err != nil {
			lastErr = err
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return nil // accepted (202); reply comes on /reply
			}
			lastErr = fmt.Errorf("atenet status %d", resp.StatusCode)
			if resp.StatusCode < 500 {
				return lastErr // hard client error; don't retry
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
	}
	return lastErr
}

// HandleReply is the sidecar-facing endpoint. The actor's egress-sidecar
// POSTs the agent's response here (outbound — the survivable direction),
// keyed by X-Request-Id.
func (b *Broker) HandleReply(w http.ResponseWriter, r *http.Request) {
	reqID := r.Header.Get(headerRequestID)
	body, _ := io.ReadAll(r.Body)
	status, _ := strconv.Atoi(r.Header.Get(headerStatus))
	if status == 0 {
		status = http.StatusOK
	}
	msg := replyMsg{status: status, contentType: r.Header.Get(headerCT), body: body}

	b.mu.Lock()
	ch := b.pending[reqID]
	b.mu.Unlock()
	if ch == nil {
		log.Printf("ingress: reply for unknown reqID=%s (client gone?)", reqID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	select {
	case ch <- msg:
	default:
	}
	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) register(reqID string, ch chan replyMsg) {
	b.mu.Lock()
	b.pending[reqID] = ch
	b.mu.Unlock()
}

func (b *Broker) unregister(reqID string) {
	b.mu.Lock()
	delete(b.pending, reqID)
	b.mu.Unlock()
}
