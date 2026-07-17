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

// Command egress-broker is the outside-the-actor half of the resumable
// egress tunnel (DESIGN.md phase 1). It accepts tunnel connections from
// egress-sidecars, holds each session's upstream connection open across
// actor suspends, and resumes actors via substrate's ateapi when a response
// arrives for a suspended, pending session.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/dberkov/substrate-poc-3/internal/ateapi"
	"github.com/dberkov/substrate-poc-3/internal/broker"
)

var (
	tunnelListen  = pflag.String("tunnel-listen", envOr("TUNNEL_LISTEN", ":9000"), "address to accept sidecar tunnel connections on")
	debugListen   = pflag.String("debug-listen", envOr("DEBUG_LISTEN", ":9001"), "address for the debug/status HTTP endpoint")
	ateapiAddr    = pflag.String("ateapi", envOr("ATEAPI_ADDR", "api.ate-system.svc:443"), "substrate ateapi gRPC address")
	ateapiInsec   = pflag.Bool("ateapi-insecure", envBool("ATEAPI_INSECURE", false), "use a plaintext (non-TLS) ateapi connection")
	sessionBuffer = pflag.Int("session-buffer-bytes", envInt("SESSION_BUFFER_BYTES", 4*1024*1024), "per-session per-direction replay buffer cap")
	watchdog      = pflag.Duration("max-suspend-watchdog", envDur("MAX_SUSPEND_WATCHDOG", 5*time.Minute), "resume a pending session held longer than this, as a backstop")
)

func main() {
	pflag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	log := slog.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lc, err := ateapi.Dial(ateapi.Config{Addr: *ateapiAddr, Insecure: *ateapiInsec})
	if err != nil {
		log.Error("dial ateapi", "err", err)
		os.Exit(1)
	}
	defer lc.Close()

	b := broker.New(broker.Config{
		Lifecycle:          lc,
		SessionBufferBytes: *sessionBuffer,
		MaxSuspendWatchdog: *watchdog,
		Logger:             log,
	})

	ln, err := net.Listen("tcp", *tunnelListen)
	if err != nil {
		log.Error("listen tunnel", "addr", *tunnelListen, "err", err)
		os.Exit(1)
	}
	log.Info("egress-broker tunnel listening", "addr", *tunnelListen)

	go serveDebug(ctx, log, b, lc)

	if err := b.Serve(ctx, ln); err != nil {
		log.Error("tunnel serve", "err", err)
		os.Exit(1)
	}
}

// serveDebug exposes the session table and manual suspend/resume, for
// deterministic demos and tests (DESIGN.md §7). The broker itself never
// initiates suspend; these endpoints are operator tools.
func serveDebug(ctx context.Context, log *slog.Logger, b *broker.Broker, lc ateapi.Lifecycle) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/debug/sessions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(b.Sessions())
	})
	// POST /debug/suspend/{atespace}/{name} and /debug/resume/{atespace}/{name}
	mux.HandleFunc("/debug/suspend/", lifecycleHandler(log, "suspend", lc.SuspendActor))
	mux.HandleFunc("/debug/resume/", lifecycleHandler(log, "resume", lc.ResumeActor))

	srv := &http.Server{Addr: *debugListen, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	log.Info("egress-broker debug listening", "addr", *debugListen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("debug serve", "err", err)
	}
}

func lifecycleHandler(log *slog.Logger, verb string, fn func(context.Context, ateapi.Ref) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		// Path tail is "{atespace}/{name}".
		tail := strings.TrimPrefix(r.URL.Path, "/debug/"+verb+"/")
		parts := strings.SplitN(tail, "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			http.Error(w, "path must be /debug/"+verb+"/{atespace}/{name}", http.StatusBadRequest)
			return
		}
		ref := ateapi.Ref{Atespace: parts[0], Name: parts[1]}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := fn(ctx, ref); err != nil {
			log.Warn("manual "+verb+" failed", "actor", ref.String(), "err", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		log.Info("manual "+verb+" ok", "actor", ref.String())
		w.WriteHeader(http.StatusNoContent)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
