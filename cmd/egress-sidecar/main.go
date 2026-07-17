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

// Command egress-sidecar runs inside the actor alongside the agent. It is
// the in-actor half of the resumable egress tunnel (DESIGN.md phase 1):
//
//   - a forward HTTP(S) proxy the agent reaches via HTTP(S)_PROXY;
//   - a tunnel client that multiplexes agent connections to the broker and
//     re-attaches them after every suspend;
//   - a suspend poller that watches the agent's /statusz and calls
//     SuspendActor when the agent is blocked on egress or idle.
//
// Its only substrate awareness is its actor identity and the two lifecycle
// calls — it never inspects payloads.
package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"log/slog"

	"github.com/spf13/pflag"

	"github.com/dberkov/substrate-poc-3/internal/activitystatus"
	"github.com/dberkov/substrate-poc-3/internal/ateapi"
	"github.com/dberkov/substrate-poc-3/internal/sidecar"
)

var (
	proxyListen = pflag.String("proxy-listen", envOr("PROXY_LISTEN", ":15001"), "address the agent points HTTP(S)_PROXY at")
	brokerAddr  = pflag.String("broker-addr", envOr("BROKER_ADDR", ""), "egress-broker tunnel address (host:port)")

	atespace    = pflag.String("atespace", envOr("ATE_ATESPACE", ""), "actor's atespace (constant per template; safe as env)")
	actorName   = pflag.String("actor-name", envOr("ATE_ACTOR_NAME", ""), "actor name; overrides --actor-id-file when set")
	actorIDFile = pflag.String("actor-id-file", envOr("ATE_ACTOR_ID_FILE", "/run/ate/actor-id"), "file substrate bind-mounts with the actor name")

	statusURL   = pflag.String("status-url", envOr("STATUS_URL", "http://127.0.0.1:9902"+activitystatus.Path), "agent activity endpoint the suspend poller reads")
	ateapiAddr  = pflag.String("ateapi", envOr("ATEAPI_ADDR", "api.ate-system.svc:443"), "substrate ateapi gRPC address")
	ateapiInsec = pflag.Bool("ateapi-insecure", envBool("ATEAPI_INSECURE", false), "use a plaintext (non-TLS) ateapi connection")
	suspendOff  = pflag.Bool("no-suspend", envBool("NO_SUSPEND", false), "disable the suspend poller (proxy/tunnel only)")

	blockedAfter = pflag.Duration("blocked-after", envDur("BLOCKED_AFTER", time.Second), "suspend after being blocked on a tool/model call this long")
	idleAfter    = pflag.Duration("idle-after", envDur("IDLE_AFTER", 0), "suspend after no invocation for this long (0 disables)")
	pingInterval = pflag.Duration("ping-interval", envDur("PING_INTERVAL", time.Second), "tunnel PING interval")
	pongTimeout  = pflag.Duration("pong-timeout", envDur("PONG_TIMEOUT", 3*time.Second), "reconnect if no PONG within this")
	sessionBuf   = pflag.Int("session-buffer-bytes", envInt("SESSION_BUFFER_BYTES", 4*1024*1024), "per-session upstream replay buffer cap")
)

func main() {
	pflag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	log := slog.Default()

	if *brokerAddr == "" {
		log.Error("--broker-addr (BROKER_ADDR) is required")
		os.Exit(1)
	}
	// identity resolves the actor Ref FRESH on every call. The name MUST NOT
	// be cached: the sidecar's memory is frozen into the ActorTemplate golden
	// snapshot, so a name read once at startup would be the golden actor's
	// name on every hydrated actor — making Suspend/ResumeActor target a
	// deleted actor and colliding all actors under one broker key. The
	// atespace is constant per template, so taking it from env is fine.
	identity := func() ateapi.Ref {
		name := *actorName
		if name == "" {
			name = readActorName(*actorIDFile)
		}
		return ateapi.Ref{Atespace: *atespace, Name: name}
	}
	if identity().Name == "" {
		log.Error("actor name unknown: set --actor-name or ensure --actor-id-file exists", "file", *actorIDFile)
		os.Exit(1)
	}
	log.Info("egress-sidecar starting", "actor", identity().String(), "broker", *brokerAddr, "proxy", *proxyListen)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := sidecar.NewClient(sidecar.ClientConfig{
		ActorID:       func() string { return identity().String() },
		BrokerAddr:    *brokerAddr,
		SessionBuffer: *sessionBuf,
		PingInterval:  *pingInterval,
		PongTimeout:   *pongTimeout,
		Logger:        log,
	})
	go client.Run(ctx)

	if !*suspendOff {
		lc, err := ateapi.Dial(ateapi.Config{Addr: *ateapiAddr, Insecure: *ateapiInsec})
		if err != nil {
			log.Error("dial ateapi", "err", err)
			os.Exit(1)
		}
		defer lc.Close()
		susp := sidecar.NewSuspender(sidecar.SuspenderConfig{
			Lifecycle:    lc,
			Actor:        identity,
			StatusURL:    *statusURL,
			BlockedAfter: *blockedAfter,
			IdleAfter:    *idleAfter,
			Logger:       log,
		})
		go susp.Run(ctx)
		log.Info("suspend poller enabled", "blockedAfter", *blockedAfter, "idleAfter", *idleAfter)
	} else {
		log.Info("suspend poller disabled")
	}

	ln, err := net.Listen("tcp", *proxyListen)
	if err != nil {
		log.Error("listen proxy", "addr", *proxyListen, "err", err)
		os.Exit(1)
	}
	proxy := sidecar.NewProxy(client, log)
	if err := proxy.Serve(ctx, ln); err != nil {
		log.Error("proxy serve", "err", err)
		os.Exit(1)
	}
}

// readActorName reads the actor name substrate bind-mounts (a single line).
func readActorName(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
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
