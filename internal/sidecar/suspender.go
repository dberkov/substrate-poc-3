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
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/dberkov/substrate-poc-3/internal/activitystatus"
	"github.com/dberkov/substrate-poc-3/internal/ateapi"
)

// Suspender polls the agent's activity endpoint and suspends the actor when
// it is provably blocked on egress or has gone idle (DESIGN.md §6). The
// sidecar owns suspend; the broker owns resume. This deliberately prototypes
// a capability substrate could grow natively (ateom/atelet polling an
// `idlez` probe like `readyz`).
type Suspender struct {
	lc ateapi.Lifecycle
	// actor returns the actor identity read FRESH on every call — never
	// cached, for the same golden-snapshot reason as Client.actorID.
	actor     func() ateapi.Ref
	statusURL string
	http      *http.Client
	log       *slog.Logger

	poll         time.Duration
	blocked      time.Duration // suspend if blocked on a tunneled call this long
	includeModel bool          // also suspend on model-call blocking (phase 2)
	idle         time.Duration // suspend after this long with no in-flight ingress request (0 disables)
	cooldown     time.Duration // min time between suspend attempts
	resumeGap    time.Duration // wall-clock gap between ticks above this means we were suspended (resume detected)

	// ingressIdle returns how long there has been no in-flight ingress
	// request (0 while one is being processed). This is the phase-3 idle
	// signal — precise and race-free because the sidecar owns the whole
	// request→reply cycle. nil disables idle-suspend.
	ingressIdle func() time.Duration

	// lastToolGen/lastModelGen are the ToolCallsStarted/ModelCallsStarted
	// values at the last tool/model-block suspend. A given call is suspended
	// at most once: on resume the same call is still in flight with the same
	// started-count, so the poller must not re-suspend before the broker's
	// response has been delivered (that would loop wake → re-suspend → wake
	// and the result would never arrive). Clock-independent — no timing grace.
	lastToolGen  uint64
	lastModelGen uint64
}

// SuspenderConfig configures a Suspender.
type SuspenderConfig struct {
	Lifecycle ateapi.Lifecycle
	// Actor returns the actor identity, read fresh on every call.
	Actor             func() ateapi.Ref
	StatusURL         string // e.g. http://127.0.0.1:9902/statusz
	PollInterval      time.Duration
	BlockedAfter      time.Duration
	IncludeModelCalls bool          // suspend on model-call blocking too (phase 2)
	IdleAfter         time.Duration // suspend after this long with no in-flight ingress request (0 disables)
	Cooldown          time.Duration
	// IngressIdle returns how long there has been no in-flight ingress
	// request. Required for idle-suspend (phase 3); nil disables it.
	IngressIdle func() time.Duration
	Logger      *slog.Logger
}

func NewSuspender(cfg SuspenderConfig) *Suspender {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BlockedAfter == 0 {
		cfg.BlockedAfter = time.Second
	}
	if cfg.Cooldown == 0 {
		cfg.Cooldown = 2 * time.Second
	}
	resumeGap := 2 * time.Second
	if cfg.PollInterval*3 > resumeGap {
		resumeGap = cfg.PollInterval * 3
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Suspender{
		lc:           cfg.Lifecycle,
		actor:        cfg.Actor,
		statusURL:    cfg.StatusURL,
		http:         &http.Client{Timeout: 2 * time.Second},
		log:          cfg.Logger,
		poll:         cfg.PollInterval,
		blocked:      cfg.BlockedAfter,
		includeModel: cfg.IncludeModelCalls,
		idle:         cfg.IdleAfter,
		cooldown:     cfg.Cooldown,
		resumeGap:    resumeGap,
		ingressIdle:  cfg.IngressIdle,
	}
}

// Run polls until ctx is cancelled.
func (s *Suspender) Run(ctx context.Context) {
	t := time.NewTicker(s.poll)
	defer t.Stop()
	var lastSuspend time.Time
	prevTick := time.Now()
	var idleGraceUntil time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		now := time.Now()
		// Detect a resume: a wall-clock gap between ticks much larger than the
		// poll interval means the actor was suspended in between (.Round(0)
		// strips the monotonic reading so this compares wall clocks, robust to
		// how the monotonic clock behaves across checkpoint/restore). After a
		// resume the ingress idle timer is stale, so grant a grace window
		// before idle-suspend can fire again — long enough for the request
		// that triggered the resume to register as in-flight. Without this the
		// poller re-suspends the just-woken actor before its request is
		// processed, and the request is lost.
		if now.Round(0).Sub(prevTick.Round(0)) > s.resumeGap {
			idleGraceUntil = now.Add(s.idle)
			s.log.Debug("resume detected; holding off idle-suspend", "until", idleGraceUntil)
		}
		prevTick = now

		// Block-suspend needs /statusz; idle-suspend needs the ingress signal.
		// A /statusz error only disables the block path, not idle.
		st, err := s.fetch(ctx)
		if err != nil {
			s.log.Debug("activity poll failed", "err", err)
		}
		reason := ""
		if err == nil {
			reason = s.decideBlocked(st)
		}
		if reason == "" && s.ingressIdle != nil && s.idle > 0 &&
			now.After(idleGraceUntil) && s.ingressIdle() >= s.idle {
			reason = reasonIdle
		}
		if reason == "" {
			continue
		}
		// Suspend each tool/model call at most once. After a suspend the
		// broker holds the upstream and wakes the actor when the response
		// arrives; on resume the SAME call is still in flight (same
		// *CallsStarted), and re-suspending now — before the reconnecting
		// tunnel has replayed the response — would loop forever and the result
		// would never reach the agent. Gate on the started-count, which is
		// clock-independent and survives the checkpoint.
		switch reason {
		case reasonToolBlocked:
			if st.ToolCallsStarted <= s.lastToolGen {
				continue // already suspended this tool call; awaiting its response
			}
		case reasonModelBlocked:
			if st.ModelCallsStarted <= s.lastModelGen {
				continue // already suspended this model call; awaiting its response
			}
		}
		// The sidecar's clock freezes with the actor across a suspend, so the
		// cooldown never counts suspended time — it only debounces repeated
		// attempts while running.
		if time.Since(lastSuspend) < s.cooldown {
			continue
		}
		switch reason {
		case reasonToolBlocked:
			s.lastToolGen = st.ToolCallsStarted
		case reasonModelBlocked:
			s.lastModelGen = st.ModelCallsStarted
		}
		lastSuspend = time.Now()
		s.suspend(reason, st)
	}
}

// Suspend reasons (also used to key the per-tool-call gate in Run).
const (
	reasonToolBlocked  = "blocked on tool call"
	reasonModelBlocked = "blocked on model call"
	reasonIdle         = "idle between turns"
)

// decideBlocked applies the mid-request block policy to a /statusz snapshot
// (idle-suspend is handled separately in Run, from the ingress signal).
func (s *Suspender) decideBlocked(st activitystatus.Status) string {
	if st.ToolCallsInFlight > 0 && st.ToolBlockedMillis >= s.blocked.Milliseconds() {
		return reasonToolBlocked
	}
	if s.includeModel && st.ModelCallsInFlight > 0 && st.ModelBlockedMillis >= s.blocked.Milliseconds() {
		return reasonModelBlocked
	}
	return ""
}

func (s *Suspender) fetch(ctx context.Context) (activitystatus.Status, error) {
	var st activitystatus.Status
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.statusURL, nil)
	if err != nil {
		return st, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// suspend fires SuspendActor. From inside the actor this RPC never returns
// cleanly — the sidecar freezes mid-call and the connection is a zombie on
// resume — so any error is expected and merely logged.
func (s *Suspender) suspend(reason string, st activitystatus.Status) {
	actor := s.actor()
	s.log.Info("suspending actor",
		"actor", actor.String(),
		"reason", reason,
		"toolBlockedMillis", st.ToolBlockedMillis,
		"modelBlockedMillis", st.ModelBlockedMillis,
		"idleMillis", st.IdleMillis,
		"tools", st.ToolCallsInFlight,
		"models", st.ModelCallsInFlight)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.lc.SuspendActor(ctx, actor); err != nil {
		s.log.Info("SuspendActor returned (error expected if suspend succeeded)", "actor", actor.String(), "err", err)
		return
	}
	s.log.Info("SuspendActor returned without error (actor may already be suspended)", "actor", actor.String())
}
