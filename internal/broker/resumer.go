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

package broker

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dberkov/substrate-poc-3/internal/ateapi"
)

// resumer serializes ResumeActor calls per actor so a burst of wake
// conditions (e.g. many sessions receiving data at once on resume) collapses
// into one in-flight RPC, mirroring atenet's singleflight resumer. The call
// runs on a detached context so it completes even if the triggering session
// goes away.
type resumer struct {
	lc  ateapi.Lifecycle
	log *slog.Logger

	mu       sync.Mutex
	inflight map[string]struct{}
}

func newResumer(lc ateapi.Lifecycle, log *slog.Logger) *resumer {
	return &resumer{lc: lc, log: log, inflight: make(map[string]struct{})}
}

// resume triggers a best-effort ResumeActor for the actor named
// "atespace/name". Concurrent calls for the same actor are deduped.
func (r *resumer) resume(actorID string) {
	r.mu.Lock()
	if _, busy := r.inflight[actorID]; busy {
		r.mu.Unlock()
		return
	}
	r.inflight[actorID] = struct{}{}
	r.mu.Unlock()

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.inflight, actorID)
			r.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := r.lc.ResumeActor(ctx, parseRef(actorID)); err != nil {
			r.log.Warn("ResumeActor failed", "actor", actorID, "err", err)
			return
		}
		r.log.Info("ResumeActor ok", "actor", actorID)
	}()
}

// parseRef splits "atespace/name" into a Ref. A bare name (no slash) yields
// an empty atespace, which is valid for global-scoped actors.
func parseRef(actorID string) ateapi.Ref {
	if i := strings.IndexByte(actorID, '/'); i >= 0 {
		return ateapi.Ref{Atespace: actorID[:i], Name: actorID[i+1:]}
	}
	return ateapi.Ref{Name: actorID}
}
