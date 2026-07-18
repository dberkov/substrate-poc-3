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

// Package agentsrv is the HTTP front end of the calculator agent actor. It
// serves POST /run (called via the ingress-broker) and drives the ADK
// runner, deduplicating by (sessionID, input) so a client request retried
// after a suspend/resume gets the same answer. It registers the generic
// activityz plugin so the sidecar can observe in-flight work.
//
// Note what is NOT here versus poc-1: no actor ID, no ateapi, no self-
// suspend, no egress broker. The egress transparency lives entirely in the
// sidecar; this server is a plain ADK host.
package agentsrv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/plugin"
	"google.golang.org/adk/runner"
	"google.golang.org/adk/session"
	"google.golang.org/genai"

	"github.com/dberkov/substrate-poc-3/internal/activityz"
)

const (
	appName = "adk-calc"
	userID  = "poc-user"
)

// Server hosts the ADK runner behind POST /run.
type Server struct {
	runner    *runner.Runner
	tracker   *activityz.Tracker
	notifyURL string
	notifier  *http.Client
	registry  *registry
}

// New builds a Server for agent a. notifyURL is the ingress-broker wake
// endpoint (empty disables notifications). The returned Server also exposes
// the activity tracker via StatusHandler.
func New(a agent.Agent, notifyURL string) (*Server, error) {
	tracker, activityPlugin, err := activityz.New("activityz")
	if err != nil {
		return nil, fmt.Errorf("activityz.New: %w", err)
	}
	r, err := runner.New(runner.Config{
		AppName:           appName,
		Agent:             a,
		SessionService:    session.InMemoryService(),
		AutoCreateSession: true,
		PluginConfig:      runner.PluginConfig{Plugins: []*plugin.Plugin{activityPlugin}},
	})
	if err != nil {
		return nil, fmt.Errorf("runner.New: %w", err)
	}
	return &Server{
		runner:    r,
		tracker:   tracker,
		notifyURL: notifyURL,
		// The notify call is infrastructure plumbing to the ingress-broker,
		// not agent egress — it must NOT traverse the egress tunnel (that
		// would be circular), so its transport explicitly ignores HTTP_PROXY.
		// DisableKeepAlives: /notify is the ingress-broker's only wake path,
		// and it fires right after a resume. A pooled connection from a
		// previous turn is dead after the suspend, and Go won't retry a POST
		// on a broken persistent connection — so dial fresh every time.
		notifier: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}},
		registry: newRegistry(),
	}, nil
}

// StatusHandler serves the activity endpoint (/statusz) the sidecar polls.
func (s *Server) StatusHandler() http.Handler { return s.tracker.Handler() }

type runReq struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
}

type runResp struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// HandleRun runs the agent for (sessionId, input), deduplicating retries.
func (s *Server) HandleRun(w http.ResponseWriter, r *http.Request) {
	var req runReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, runResp{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.SessionID == "" || req.Input == "" {
		writeJSON(w, http.StatusBadRequest, runResp{Error: "sessionId and input are required"})
		return
	}

	key := dedupKey(req.SessionID, req.Input)
	e, isNew := s.registry.getOrCreate(key)

	if !isNew {
		log.Printf("run: session=%s key=%s joining in-flight/cached", req.SessionID, key)
		result, errMsg, ok := e.wait(r.Context())
		if !ok {
			writeJSON(w, http.StatusServiceUnavailable, runResp{Error: "client cancelled while waiting for cached result"})
			return
		}
		if errMsg != "" {
			writeJSON(w, http.StatusInternalServerError, runResp{Error: errMsg})
			return
		}
		writeJSON(w, http.StatusOK, runResp{Result: result})
		return
	}

	log.Printf("run: session=%s key=%s input=%q starting", req.SessionID, key, req.Input)

	// Detach from the request context: if the ingress connection drops when
	// the actor is suspended mid-run, the run still completes on resume,
	// caches the result, and notifies the broker.
	runCtx := context.Background()
	msg := &genai.Content{Role: "user", Parts: []*genai.Part{{Text: req.Input}}}

	var final, errMsg string
	for ev, err := range s.runner.Run(runCtx, userID, req.SessionID, msg, agent.RunConfig{}) {
		if err != nil {
			errMsg = err.Error()
			log.Printf("run: session=%s error=%v", req.SessionID, err)
			break
		}
		if ev == nil || !ev.IsFinalResponse() || ev.Content == nil {
			continue
		}
		for _, p := range ev.Content.Parts {
			if p.Text != "" {
				final = p.Text
			}
		}
	}

	e.complete(final, errMsg)
	s.notifyBroker(req.SessionID, req.Input)
	log.Printf("run: session=%s key=%s result=%q err=%q", req.SessionID, key, final, errMsg)

	if errMsg != "" {
		writeJSON(w, http.StatusInternalServerError, runResp{Error: errMsg})
		return
	}
	writeJSON(w, http.StatusOK, runResp{Result: final})
}

// notifyBroker tells the ingress-broker a result is ready, so it can wake a
// parked client request. This is the ingress-broker's ONLY wake path, so it
// retries: the call fires right after a resume, when the direct (untunneled)
// network path may briefly reset a connection. Dropping it would strand the
// client until its own timeout.
func (s *Server) notifyBroker(sessionID, input string) {
	if s.notifyURL == "" {
		return
	}
	body, _ := json.Marshal(runReq{SessionID: sessionID, Input: input})
	const attempts = 5
	for i := 1; i <= attempts; i++ {
		req, err := http.NewRequest(http.MethodPost, s.notifyURL, bytes.NewReader(body))
		if err != nil {
			log.Printf("notify: build request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.notifier.Do(req)
		if err == nil {
			resp.Body.Close()
			log.Printf("notify %s: status=%d attempt=%d", s.notifyURL, resp.StatusCode, i)
			return
		}
		log.Printf("notify %s: attempt=%d failed: %v", s.notifyURL, i, err)
		if i < attempts {
			time.Sleep(time.Duration(i) * 300 * time.Millisecond)
		}
	}
	log.Printf("notify %s: giving up after %d attempts", s.notifyURL, attempts)
}

func writeJSON(w http.ResponseWriter, status int, body runResp) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
