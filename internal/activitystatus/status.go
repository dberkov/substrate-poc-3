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

// Package activitystatus is the wire contract for the agent's activity
// endpoint (/statusz). It has no ADK dependency so the sidecar can decode
// the report without linking the agent framework: the activityz plugin (in
// the agent process) produces it; the sidecar's suspend poller consumes it.
package activitystatus

// Path is the conventional HTTP path the activity plugin serves and the
// sidecar polls.
const Path = "/statusz"

// Status is a generic snapshot of an agent's in-flight work. It is
// deliberately framework-neutral in shape; only the producer is ADK-specific.
type Status struct {
	// InvocationsInFlight is the number of agent runs (user turns) currently
	// executing.
	InvocationsInFlight int `json:"invocationsInFlight"`
	// ToolCallsInFlight is the number of tool executions currently running.
	ToolCallsInFlight int `json:"toolCallsInFlight"`
	// ToolCallsStarted is a monotonic count of tool executions ever started.
	// It lets a suspend poller suspend each tool call at most once: on resume
	// the same call is still in flight with the same started-count, so there
	// is nothing new to suspend for. Survives checkpoint/restore (it's in the
	// agent's checkpointed memory).
	ToolCallsStarted uint64 `json:"toolCallsStarted"`
	// ModelCallsInFlight is the number of LLM calls currently running.
	ModelCallsInFlight int `json:"modelCallsInFlight"`
	// ModelCallsStarted is a monotonic count of LLM calls ever started —
	// the model-call analogue of ToolCallsStarted, so a suspend poller can
	// suspend each LLM call at most once (phase 2, when LLM traffic is
	// tunneled).
	ModelCallsStarted uint64 `json:"modelCallsStarted"`
	// ToolBlockedMillis is the age of the oldest in-flight tool call, or 0 if
	// none. The suspend poller watches this in phase 1 (only tool calls are
	// tunneled, so only they are safe to suspend during).
	ToolBlockedMillis int64 `json:"toolBlockedMillis"`
	// ModelBlockedMillis is the age of the oldest in-flight model (LLM) call,
	// or 0 if none. Watched once LLM traffic is tunneled too (phase 2).
	ModelBlockedMillis int64 `json:"modelBlockedMillis"`
	// IdleMillis is how long since the last activity ended, reported only
	// when nothing is in flight (0 otherwise).
	IdleMillis int64 `json:"idleMillis"`
}
