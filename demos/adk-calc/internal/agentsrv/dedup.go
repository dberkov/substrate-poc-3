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

package agentsrv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// registry deduplicates concurrent or repeated runs for the same
// (sessionID, input) pair. This supports the ingress side: when the actor is
// suspended mid-run, the client's parked request is retried after resume and
// must get the same answer. Entries are never evicted — the lifetime is the
// actor process, and gVisor snapshots preserve this map across suspend/
// resume so a retry after a wake fetches the same answer cheaply.
type registry struct {
	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	done   chan struct{}
	result string
	errMsg string
}

func newRegistry() *registry {
	return &registry{entries: make(map[string]*entry)}
}

// getOrCreate returns the entry for key. isNew is true iff this call created
// it, in which case the caller must eventually invoke complete().
func (r *registry) getOrCreate(key string) (e *entry, isNew bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.entries[key]; ok {
		return existing, false
	}
	e = &entry{done: make(chan struct{})}
	r.entries[key] = e
	return e, true
}

func (e *entry) complete(result, errMsg string) {
	e.result = result
	e.errMsg = errMsg
	close(e.done)
}

func (e *entry) wait(ctx context.Context) (result, errMsg string, ok bool) {
	select {
	case <-e.done:
		return e.result, e.errMsg, true
	case <-ctx.Done():
		return "", "", false
	}
}

func dedupKey(sessionID, input string) string {
	sum := sha256.Sum256([]byte(input))
	return sessionID + ":" + hex.EncodeToString(sum[:8])
}
