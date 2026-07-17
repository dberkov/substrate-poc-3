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

// Package activityz is a generic ADK plugin that tracks the agent's
// in-flight work (invocations, tool calls, model calls) and serves it at
// /statusz. It contains NO substrate awareness — it is plain agent
// introspection any deployment might use. The egress-sidecar polls this
// endpoint to make suspend decisions, keeping all lifecycle logic out of
// the agent process (DESIGN.md §6).
package activityz

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/model"
	"google.golang.org/adk/plugin"
	"google.golang.org/adk/tool"
	"google.golang.org/genai"

	"github.com/dberkov/substrate-poc-3/internal/activitystatus"
)

// Tracker holds the live activity counters. Safe for concurrent use.
type Tracker struct {
	mu             sync.Mutex
	invocations    int
	toolCalls      int
	toolCallsTotal uint64 // monotonic count of tool calls ever started
	modelCalls     int
	toolBusySince  time.Time // when tool in-flight last went 0 -> >0
	modelBusy      time.Time // when model in-flight last went 0 -> >0
	lastActivity   time.Time
}

// New returns a Tracker and the ADK Plugin that feeds it. Register the
// plugin in runner.Config.PluginConfig.Plugins and serve Tracker.Handler
// on a loopback port.
func New(name string) (*Tracker, *plugin.Plugin, error) {
	t := &Tracker{lastActivity: time.Now()}
	p, err := plugin.New(plugin.Config{
		Name:                name,
		BeforeRunCallback:   t.beforeRun,
		AfterRunCallback:    t.afterRun,
		BeforeToolCallback:  t.beforeTool,
		AfterToolCallback:   t.afterTool,
		BeforeModelCallback: t.beforeModel,
		AfterModelCallback:  t.afterModel,
	})
	if err != nil {
		return nil, nil, err
	}
	return t, p, nil
}

func (t *Tracker) beforeRun(agent.InvocationContext) (*genai.Content, error) {
	t.mu.Lock()
	t.invocations++
	t.lastActivity = time.Now()
	t.mu.Unlock()
	return nil, nil
}

func (t *Tracker) afterRun(agent.InvocationContext) {
	t.mu.Lock()
	if t.invocations > 0 {
		t.invocations--
	}
	t.lastActivity = time.Now()
	t.mu.Unlock()
}

func (t *Tracker) beforeTool(agent.ToolContext, tool.Tool, map[string]any) (map[string]any, error) {
	t.mu.Lock()
	if t.toolCalls == 0 {
		t.toolBusySince = time.Now()
	}
	t.toolCalls++
	t.toolCallsTotal++
	t.lastActivity = time.Now()
	t.mu.Unlock()
	return nil, nil
}

func (t *Tracker) afterTool(agent.ToolContext, tool.Tool, map[string]any, map[string]any, error) (map[string]any, error) {
	t.mu.Lock()
	if t.toolCalls > 0 {
		t.toolCalls--
	}
	if t.toolCalls == 0 {
		t.toolBusySince = time.Time{}
	}
	t.lastActivity = time.Now()
	t.mu.Unlock()
	return nil, nil
}

func (t *Tracker) beforeModel(agent.CallbackContext, *model.LLMRequest) (*model.LLMResponse, error) {
	t.mu.Lock()
	if t.modelCalls == 0 {
		t.modelBusy = time.Now()
	}
	t.modelCalls++
	t.lastActivity = time.Now()
	t.mu.Unlock()
	return nil, nil
}

func (t *Tracker) afterModel(agent.CallbackContext, *model.LLMResponse, error) (*model.LLMResponse, error) {
	t.mu.Lock()
	if t.modelCalls > 0 {
		t.modelCalls--
	}
	if t.modelCalls == 0 {
		t.modelBusy = time.Time{}
	}
	t.lastActivity = time.Now()
	t.mu.Unlock()
	return nil, nil
}

// Snapshot returns the current status.
func (t *Tracker) Snapshot() activitystatus.Status {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := activitystatus.Status{
		InvocationsInFlight: t.invocations,
		ToolCallsInFlight:   t.toolCalls,
		ToolCallsStarted:    t.toolCallsTotal,
		ModelCallsInFlight:  t.modelCalls,
	}
	if !t.toolBusySince.IsZero() {
		s.ToolBlockedMillis = time.Since(t.toolBusySince).Milliseconds()
	}
	if !t.modelBusy.IsZero() {
		s.ModelBlockedMillis = time.Since(t.modelBusy).Milliseconds()
	}
	if t.invocations == 0 && t.toolCalls+t.modelCalls == 0 {
		s.IdleMillis = time.Since(t.lastActivity).Milliseconds()
	}
	return s
}

// Handler serves the snapshot as JSON at activitystatus.Path.
func (t *Tracker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(activitystatus.Path, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(t.Snapshot())
	})
	return mux
}
