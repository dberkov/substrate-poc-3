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

// Command agent-server runs the vanilla calculator ADK agent inside the
// actor. It serves POST /run + /readyz on :80 (atenet-routed) and the
// generic activity endpoint /statusz on a loopback port the egress-sidecar
// polls. It has no substrate awareness — egress transparency is provided
// entirely by the sidecar and HTTP_PROXY.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/dberkov/substrate-poc-3/demos/adk-calc/internal/agent"
	"github.com/dberkov/substrate-poc-3/demos/adk-calc/internal/agentsrv"
	"github.com/dberkov/substrate-poc-3/internal/activitystatus"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()

	a, err := agent.Build(ctx)
	if err != nil {
		log.Fatalf("build agent: %v", err)
	}
	srv, err := agentsrv.New(a)
	if err != nil {
		log.Fatalf("new server: %v", err)
	}

	var ready atomic.Bool

	// Loopback activity endpoint for the sidecar's suspend poller.
	statusAddr := envOr("STATUS_LISTEN", "127.0.0.1:9902")
	go func() {
		log.Printf("activity endpoint listening on %s%s", statusAddr, activitystatus.Path)
		if err := http.ListenAndServe(statusAddr, srv.StatusHandler()); err != nil {
			log.Fatalf("status server: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/run", srv.HandleRun)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	ready.Store(true)
	// Phase 3: the egress-sidecar owns the actor's :80 (atenet-routed) and
	// forwards here over loopback, so the agent listens on a private port.
	listen := envOr("LISTEN_ADDR", ":8080")
	log.Printf("agent-server listening on %s", listen)
	log.Fatal(http.ListenAndServe(listen, mux))
}
