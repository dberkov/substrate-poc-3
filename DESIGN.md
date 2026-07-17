# substrate-poc-3: Suspend/Resume-Transparent Egress for Agent Substrate

**Status:** concept approved, pre-implementation
**Language:** Go
**Target:** GKE cluster running [Agent Substrate](https://github.com/agent-substrate/substrate)
**Predecessor:** [substrate-poc-1 (`adk1` branch)](https://github.com/dberkov/substrate-poc-1/tree/adk1)

## 1. Goal

Make an ADK agent's **egress traffic 100% unaware of substrate suspend/resume**.

In poc-1 the agent itself was substrate-aware: it stamped `X-Actor-Id` on MCP
requests, called `SuspendActor` on itself after `tools/call`, and relied on the
egress-broker's request-level dedup + JSON-RPC id rewriting to replay cached
responses after resume. This PoC removes all of that. The agent is a vanilla
ADK binary whose only deployment-specific configuration is two standard proxy
environment variables. The MCP server and LLM API are equally unaware — they
never observe a disconnect, retry, or duplicate request.

Client → agent (ingress) remains suspend-aware in phase 1 (same as poc-1) and
becomes transparent in phase 3.

## 2. Substrate facts this design relies on (verified in source)

All references are to the substrate repo at its current HEAD; pin the exact
commit when implementation starts (substrate is pre-alpha, APIs unstable).

1. **An actor is one gVisor sandbox** holding a `pause` container plus up to 10
   app containers (`pkg/api/v1alpha1/actortemplate_types.go`, `MaxItems=10`).
   All containers share a single network namespace and therefore a single
   gVisor netstack and `localhost` (`cmd/atelet/oci.go` gives every container
   the same netns path).
2. **Suspend = `runsc checkpoint` of the whole sandbox**; the user-space
   netstack is part of the checkpoint. The interior network config is a fixed
   constant on every worker pod (`eth0` = `169.254.17.2/30`,
   `cmd/ateom-gvisor/main.go setupActorNetwork`), so the frozen config stays
   valid after cross-pod restore. **Consequence: loopback TCP connections
   between containers in the same actor survive suspend/resume, including
   resume on a different worker pod.** This is the foundation of the design
   (and the subject of the phase-0 validation, §9).
3. **External TCP connections die on suspend, silently.** ateom tears down the
   veth pair and nftables NAT after checkpoint; egress is masqueraded behind
   the ephemeral worker-pod IP. `runsc` runs with `-allow-connected-on-save`,
   which only means checkpointing doesn't fail with connected sockets — on
   restore they are zombies (no FIN/RST is ever delivered).
4. **Substrate has no egress machinery today** — just broad masquerade, with a
   source comment saying a future "AgentGateway" phase should replace it with
   transparent TCP capture (`cmd/ateom-gvisor/main.go` ~691). This PoC
   prototypes that gap.
5. **Ingress wake-on-request already exists**: atenet runs stock Envoy with a
   custom Go ext_proc server (`cmd/atenet/internal/router/extproc.go`) that
   calls `Control.ResumeActor` (singleflight-deduped) for every inbound
   request, then rewrites `:authority` to `<workerIP>:80`. Note: the resume
   logic lives in the Go ext_proc companion, **not** in xDS — xDS only
   programs listeners/clusters. We mirror this pattern on egress: stock
   Envoy where it earns its place (phase 4), substrate-aware logic in Go.
6. **Lifecycle API**: gRPC `Control` service (`pkg/proto/ateapipb`):
   `SuspendActor`, `PauseActor`, `ResumeActor` (idempotent fast-path when
   already RUNNING), etc.
7. **Actor identity** is provided as a bind-mounted file `/run/ate/actor-id`
   (not env — env is frozen into the golden snapshot). Containers may declare
   an HTTP `readyz` probe in the ActorTemplate.

## 3. Architecture

```
┌─────────────────────── actor (one gVisor sandbox) ───────────────────────┐
│                                                                          │
│  ┌────────────────────────┐   loopback TCP        ┌──────────────────┐   │
│  │ adk agent (vanilla)    │ ────────────────────▶ │  egress-sidecar  │   │
│  │  + activity plugin     │  survives suspend/    │                  │   │
│  │  HTTP_PROXY=localhost: │  resume byte-for-byte │  - proxy listener│   │
│  │   15001                │                       │  - tunnel client │   │
│  │  /statusz ◀────────────┼──── idle polling ─────│  - idle poller   │   │
│  └────────────────────────┘                       │  - suspend caller│   │
│                                                   └────────┬─────────┘   │
└─────────────────────────────────────────────────────────── │ ────────────┘
                                                             │
                                     SuspendActor ◀──────────┤
                                     (ateapi, fire-and-forget│
                                      via masquerade path)   │
                                                             │
                              breaks on every suspend;       │  resumable
                              sidecar re-dials + ATTACHes    ▼  tunnel
                                             ┌────────────────────────────┐
                                             │       egress-broker (Go)   │
                                             │  - tunnel server           │
                                             │  - per-session buffers     │
                                             │  - holds upstream TCP open │──▶ ateapi
                                             │    across suspends         │    ResumeActor
                                             │  - wake policy             │
                                             │  - debug endpoints         │
                                             └─────────────┬──────────────┘
                                                 live TCP  │  stays open while
                                                           │  actor is suspended
                                                           ▼
                                              MCP server / LLM API (HTTPS)
                                              (fully unaware)
```

### Division of lifecycle authority

- **Sidecar owns suspend.** Co-located with the actor, it polls the agent's
  activity endpoint, applies the suspend policy, and calls `SuspendActor`.
- **Broker owns resume.** It is the only component that can observe wake
  conditions (upstream bytes/close arriving for a detached pending session),
  and calls `ResumeActor`.

Neither component needs the other's cooperation to act.

### Awareness table

| Component  | Substrate awareness |
|------------|---------------------|
| adk agent  | **none** — real destination URLs + standard `HTTP(S)_PROXY`/`NO_PROXY` env; the activity plugin is generic introspection (no substrate imports) |
| MCP server | **none** — sees one connection, one request, one response; a suspended peer looks like a slow client (TCP flow control) |
| LLM API    | **none** — same, with end-to-end TLS (no MITM) |
| sidecar    | full (infrastructure): reads `/run/ate/actor-id`, calls `SuspendActor` |
| broker     | full (infrastructure): calls `ResumeActor` |

## 4. Traffic interception: standard proxy env vars

The agent container gets:

```
HTTP_PROXY=http://127.0.0.1:15001
HTTPS_PROXY=http://127.0.0.1:15001
```

(The literal IP rather than `localhost`: the gVisor actor image has no
`/etc/hosts`, so `localhost` would leak to cluster DNS and fail.)

```
NO_PROXY=localhost,127.0.0.1
```

Go's `net/http` honors these by default (`ProxyFromEnvironment`), which covers
the ADK, the genai SDK, and the MCP Go SDK — as do essentially all HTTP client
libraries in other ecosystems. The agent keeps its **real** destination URLs.

- `http://` destinations: the client sends the absolute-form request to the
  sidecar; the sidecar learns the destination from the request line. No
  per-destination configuration anywhere.
- `https://` destinations: the client sends `CONNECT host:443`, then runs TLS
  **through** the tunnel to the real server. The sidecar/broker shuttle opaque
  bytes; the agent validates the real server certificate. The agent's TLS
  state is inside the checkpoint and the server's is on the live socket the
  broker keeps open, so the TLS stream survives suspend/resume without either
  endpoint noticing.

Clients that ignore proxy env vars (rare; e.g. raw gRPC) fall through to the
masquerade path: they still work, just without suspend protection. Transparent
interception (nftables REDIRECT inside the sandbox — substrate's own
"AgentGateway" sketch) is deliberately out of scope until a later phase.

**Untunneled connections and keep-alive pooling (phase-1 caveat).** Any
connection NOT routed through the sidecar (in phase 1 that's the LLM traffic —
`HTTPS_PROXY` is intentionally unset so Gemini stays direct) uses the
masquerade path and therefore dies on every suspend. HTTP clients pool
keep-alive connections, so after a suspend the next call on that pool reuses a
dead connection and fails (`connection reset by peer`); Go does not auto-retry
POSTs on a broken persistent connection. Two ways to live with this: (a) tunnel
the traffic (phase 2 — the connection then survives suspend and pooling is
fine), or (b) for a connection that must stay direct, disable keep-alives so
each call dials fresh after resume. The adk-calc demo does (b) for its Gemini
client as a phase-1 stopgap. This is only safe because the suspend policy fires
on *tool-call* blocking only, so an LLM call is never itself interrupted
mid-flight — only idle pooled LLM connections are lost.

## 5. The resumable tunnel protocol (sidecar ⇄ broker)

Resumption is done at the **application byte-stream level with explicit
offsets** — not by reconstructing TCP state, which would require owning the
netstack and is fragile across gVisor restore.

One **session** = one agent-side TCP connection (one proxied request pipeline
or one CONNECT tunnel). Sessions are multiplexed over a single sidecar↔broker
TCP connection (one tunnel connection per actor; re-established after every
resume).

Frames (length-prefixed binary; exact encoding decided at implementation):

| Frame | Direction | Fields | Purpose |
|---|---|---|---|
| `HELLO`  | S→B | actorID, protocol version | Sent once per tunnel connection. actorID read from `/run/ate/actor-id`. |
| `OPEN`   | S→B | sessionID, target (host:port), mode (http-proxy \| connect) | New agent connection; broker dials the target. |
| `ATTACH` | S→B | sessionID, deliveredOffset | Re-adopt an existing session after resume: "I have delivered bytes to the agent up to offset N; replay from there." |
| `DATA`   | both | sessionID, offset, bytes | Payload. Offsets are cumulative per session per direction. |
| `ACK`    | both | sessionID, offset | Cumulative ack; sender trims its replay buffer up to the acked offset. |
| `CLOSE`  | both | sessionID, direction, reason | Half-close/close propagation (delivered in-order relative to DATA, subject to the wake policy). |
| `PING`/`PONG` | both | timestamp | Liveness; primary mechanism for detecting a zombie tunnel after restore. |
| `STATUS` | S→B | activity snapshot | Optional: forwarded `/statusz` info for broker observability (not used for decisions). |

Key mechanics:

- **Symmetric buffering.** Each side retains un-acked `DATA` it has sent, so
  a suspend landing mid-request-body (agent→upstream) replays exactly like one
  landing mid-response (upstream→agent). Un-acked bytes held by the sidecar
  are inside the checkpoint — preserved by construction.
- **Backpressure.** The broker caps the per-session buffer; when full it stops
  reading from the upstream socket, and TCP flow control makes the server see
  an ordinary slow client. No unbounded memory, no protocol-level pause frames
  needed in v1.
- **Zombie detection.** After restore, the sidecar's old tunnel socket is a
  checkpointed zombie (writes appear to succeed locally, nothing moves).
  Missed `PONG`s trigger an immediate re-dial + `HELLO` + `ATTACH` for every
  live session. Keep the ping interval tight (~1s) since resume-to-replay
  latency sits on the critical path of the demo.
- **Session lifetime ≠ request lifetime.** HTTP clients pool keep-alive
  connections; a session typically stays open across many request/response
  exchanges and long idle gaps. A session ends when the agent closes it (pool
  eviction, process exit), the upstream closes it (idle reaper — see wake
  policy), or broker GC reaps an orphan (actor deleted, sidecar never
  re-attached; generous TTL).

## 6. Suspend policy (sidecar)

The agent runs a generic **activity plugin** built on the ADK Plugin API
(`google.golang.org/adk` `plugin` package — verified present at v1.4.0):
`BeforeRunCallback`/`AfterRunCallback` bracket each invocation,
`BeforeToolCallback`/`AfterToolCallback` bracket tool executions,
`BeforeModelCallback`/`AfterModelCallback` bracket LLM calls. The plugin keeps
atomic in-flight counters + start timestamps and serves them at
`GET localhost:9902/statusz`. It contains no substrate imports — it is generic
introspection any deployment could use.

The sidecar polls `/statusz` and calls `SuspendActor` when either holds:

1. **Blocked:** a *tool* call has been in flight for > `BLOCKED_AFTER` (1 s in
   the demo) — the actor is provably waiting on egress the broker is holding.
   (Model/LLM calls are excluded in phase 1 since Gemini is not tunneled; they
   join once `IncludeModelCalls` is enabled in phase 2.)
2. **Idle:** no ADK invocation in flight for > `IDLE_AFTER`. **Off in phase 1**
   (`IDLE_AFTER=0`): because the sidecar can't observe the ingress side, a bare
   "no invocation for N" timer races the `/notify`→retry that delivers the
   client's reply and can suspend before it's served. So the actor stays
   RUNNING between turns and suspends only while blocked on a tool call. Phase
   3 combines ingress+egress visibility to make idle-suspend precise and safe.

Notes:

- **Suspend each tool call at most once.** After a suspend, the broker holds
  the upstream and wakes the actor when the response arrives; on resume the
  *same* tool call is still in flight (its `afterTool` fires only once the
  reconnecting tunnel replays the response). If the poller re-suspended then,
  it would loop wake→re-suspend→wake and the result would never reach the
  agent. The plugin exposes a monotonic `ToolCallsStarted`; the sidecar
  records the value at each tool-block suspend and won't suspend again until a
  *new* tool call starts. This is clock-independent — no timing grace, so it's
  robust to how the monotonic clock behaves across checkpoint/restore.

- `SuspendActor` from inside the actor **never returns cleanly**: the caller
  freezes mid-RPC and the connection is a zombie on resume (observed in
  poc-1). Fire-and-forget with a timeout; log "error expected on success".
- Frozen clocks are self-consistent: idle timers do not accumulate across a
  suspension. If a resume delivers only a buffered connection-teardown and no
  agent activity follows, the sidecar will legitimately re-suspend — a
  wake→deliver→re-sleep cycle is correct behavior.
- The broker makes **no** suspend decisions. (An earlier draft had the broker
  infer "blocked" from outbound-then-silence on opaque bytes; the plugin's
  ground truth supersedes the heuristic.)
- **Future direction (out of scope):** substrate itself could own this —
  ateom/atelet polling a declared `idlez` probe (analogous to the existing
  `readyz`) and making the suspend decision natively. The sidecar's
  poll-and-suspend loop is a deliberate prototype of that; if substrate grows
  the capability, that code is deleted from the sidecar and nothing else
  changes. Possible upstream contribution.
- Policy is ADK-specific by way of the plugin; the transport layer (§4–5) is
  framework-agnostic. Other frameworks would supply their own activity source
  or rely on operator/debug-driven suspend.

## 7. Wake policy (broker)

For each session the broker tracks whether it is **pending** (outbound bytes
sent upstream since the last inbound data — i.e. a request is in flight) or
**quiescent**.

| Event on a detached session | Pending session | Quiescent session |
|---|---|---|
| Inbound data bytes arrive | **ResumeActor** | buffer; **do not wake** (†) |
| Upstream close / RST | **ResumeActor** (agent must see the error and retry) | buffer the close; deliver on next attach; do not wake |

(†) A quiescent session receiving data is rare (server-initiated push on an
idle keep-alive connection, TLS `close_notify` prelude). Buffer within the cap;
deliver whenever the actor next resumes for a real reason.

This wake policy is what prevents **spurious wakes**: idle pooled connections
being reaped by server idle-timeouts (the normal fate of keep-alive
connections during long suspensions) must not resurrect the actor. The agent
simply finds a closed pooled connection on next use and redials — standard
client behavior, zero awareness.

Safety nets:

- **Wake-on-close for pending sessions** also self-heals the half-sent-request
  edge (suspend froze the agent mid-body-write; upstream's read timeout
  eventually produces a response or close → wake → the frozen write completes
  → normal error handling in the agent).
- **Max-suspend watchdog:** any session pending for longer than N minutes
  triggers a resume regardless, so no policy bug can strand an actor with a
  request in flight.
- `ResumeActor` calls are singleflight-deduped per actor (mirroring atenet's
  resumer) and cheap when the actor is already RUNNING.

Debug endpoints on the broker (deterministic demos/tests):
`POST /debug/suspend/{actorID}`, `POST /debug/resume/{actorID}`,
`GET /debug/sessions`.

## 8. Decisions log

| Decision | Choice | Rationale |
|---|---|---|
| Broker technology | **Pure Go; no Envoy in phases 1–3** | The broker's essence — sessions outliving connections, upstream lifetime decoupled from downstream, cross-reconnect re-binding — fights Envoy's thread-pinned, lifetime-coupled architecture. A native C++ filter is realistically 4–8 weeks + a custom Envoy build + unstable internal APIs; proxy-wasm structurally cannot hold an upstream connection after its downstream dies; ext_proc never owns connection lifecycle. atenet itself keeps Envoy stock and puts substrate logic in Go. Envoy returns in phase 4 for what it is good at. |
| Resumption granularity | **Application-level byte offsets** | TCP-sequence reconstruction would require owning the netstack; offsets + replay buffers are trivial in Go and testable in isolation. |
| Interception | **`HTTP(S)_PROXY` env** | Out-of-the-box MCP/LLM clients work unmodified with real URLs and end-to-end TLS; sidecar learns destinations dynamically. `localhost` URL rewriting (poc-1 style) is per-destination and breaks TLS validation; transparent capture deferred. |
| Suspend authority | **Sidecar (polls agent activity plugin)** | Ground-truth signal beats traffic heuristics; substrate-awareness in infrastructure containers is acceptable; prototypes a future native substrate `idlez` capability. |
| Resume authority | **Broker** | Only component that observes upstream activity for detached sessions. |
| Broker auto-suspend | **Removed** | Superseded by plugin ground truth; keep-alive pooling makes "response finished" invisible to an opaque-byte broker. |

## 9. Risks and phase-0 validation

**R1 — the load-bearing assumption.** Loopback TCP across suspend/resume is
strongly supported by the source (checkpointed netstack, constant interior
config) but `-allow-connected-on-save` is documented as a workaround for "a
bug in networking resumption", so prove it before writing broker code:

> **Phase 0 experiment:** an actor with two trivial containers holding an open
> loopback TCP connection with in-flight, half-acked application data; force
> suspend; resume (ideally on a different worker: suspend → occupy the
> original worker → resume); verify bytes continue to flow intact. Also hold
> an *external* connection open through suspend to confirm the sandbox
> tolerates restoring a zombie external socket, and measure how the zombie
> manifests to the app (stuck writes vs. eventual error) to calibrate the
> PING interval.

**R2 — upstream max-request-duration limits.** Substrate resume is ~1 s, so
buffered-response windows are tiny and in-flight requests keep the connection
non-idle from the server's view. What remains is the server/gateway's own cap
on total request duration for very long tool calls — orthogonal to substrate
(hits a never-suspended client identically). Degradation: error propagates to
the agent, standard client retry. Document, don't engineer around.

**R3 — suspend races.** Suspend can land at any protocol stage. Safe by
construction (un-acked bytes are checkpointed with the sidecar), but the test
matrix must include: suspend with request half-sent, response half-received,
response fully buffered but un-acked, session idle, tunnel mid-PING, and
suspend racing an inbound atenet request.

**R4 — substrate instability.** Pre-alpha; pin the substrate commit (and the
runsc build referenced by its SandboxConfig) exactly as poc-1 did.

**R5 — MCP standalone SSE.** poc-1 had to disable the MCP client's standalone
SSE stream and rewrite JSON-RPC ids on replay. The byte tunnel makes both
hacks unnecessary (streams resume from offsets; ids never change because the
request is sent exactly once). Verify SSE-over-tunnel explicitly and re-enable
the default client behavior.

## 10. Phase plan

- **Phase 0 — validation (throwaway):** the R1 experiment. Go/no-go gate.
- **Phase 1 — core PoC:** tunnel protocol, sidecar, Go broker; vanilla ADK
  agent + activity plugin; HTTP MCP server (poc-1's 20s calculator for demo
  parity); sidecar-driven suspend, broker-driven resume; debug endpoints.
  Client side remains poc-1-style (suspend-aware, via atenet).
- **Phase 2 — HTTPS + LLM:** CONNECT support in sidecar/broker; MCP server
  over HTTPS; point `HTTPS_PROXY` at the sidecar so Gemini traffic rides the
  same tunnel — suspend during LLM calls with zero SDK changes.
- **Phase 3 — transparent ingress:** client unawareness. atenet already wakes
  on request; add parked-request replay (offset-tunnel treatment for the
  inbound leg, likely a second listener in the same sidecar). Combining
  ingress+egress visibility also makes the idle-suspend rule exact ("no
  in-flight inbound AND no egress activity"), allowing a much shorter idle
  window.
- **Phase 4 (optional) — Envoy composition:** stock Envoy around the Go broker
  for egress policy (CONNECT target allow-listing), mTLS, observability, xDS-
  driven routing — mirroring atenet's stock-Envoy + Go-companion pattern.
- **Stretch:** transparent capture (nftables REDIRECT in-sandbox, SNI-based
  naming) for proxy-ignorant clients; `PauseActor` tiering (local snapshot for
  short stalls, full suspend for long idleness); upstream `idlez` proposal.
