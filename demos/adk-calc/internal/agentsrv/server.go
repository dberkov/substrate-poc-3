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
// serves POST /run and drives the ADK runner, and registers the generic
// activityz plugin so the sidecar can observe in-flight work.
//
// As of phase 3 this is a plain ADK host: the egress-sidecar forwards
// requests to it over loopback (which survives suspend), so there is no
// dedup registry and no /notify — the sidecar delivers the response to the
// ingress-broker out-of-band. Nothing here is substrate-aware.
package agentsrv

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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
	runner  *runner.Runner
	tracker *activityz.Tracker
}

// New builds a Server for agent a.
func New(a agent.Agent) (*Server, error) {
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
	return &Server{runner: r, tracker: tracker}, nil
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

// HandleRun runs the agent for (sessionId, input) and returns the result.
// The request arrives over the sidecar's loopback connection, which survives
// suspend/resume, so r.Context() stays live across a suspend and the run
// simply continues — no dedup or notify needed.
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

	log.Printf("run: session=%s input=%q starting", req.SessionID, req.Input)
	msg := &genai.Content{Role: "user", Parts: []*genai.Part{{Text: req.Input}}}

	var final, errMsg string
	for ev, err := range s.runner.Run(r.Context(), userID, req.SessionID, msg, agent.RunConfig{}) {
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

	log.Printf("run: session=%s result=%q err=%q", req.SessionID, final, errMsg)
	if errMsg != "" {
		writeJSON(w, http.StatusInternalServerError, runResp{Error: errMsg})
		return
	}
	writeJSON(w, http.StatusOK, runResp{Result: final})
}

func writeJSON(w http.ResponseWriter, status int, body runResp) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
