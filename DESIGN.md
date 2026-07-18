# substrate-poc-3: Design

**Status:** implemented and verified on GKE.
**Language:** Go.
**Target:** a GKE cluster running [Agent Substrate](https://github.com/agent-substrate/substrate).

## 1. Goal

Make a Google ADK agent's **entire network I/O transparent to Agent Substrate
suspend/resume** — so an actor can be checkpointed and restored (on any worker)
in the middle of a request without the agent, the client, the MCP server, or
the LLM noticing.

Two directions:

- **Egress (agent → MCP/LLM):** the agent is a vanilla ADK binary whose only
  suspend/resume-related configuration is standard `HTTP(S)_PROXY` environment
  variables. Tool calls and LLM calls survive suspend/resume; the MCP server
  and LLM never observe a disconnect, retry, or duplicate request.
- **Ingress (client → agent):** the client is a plain HTTP caller. It sends a
  request and receives the answer even though the actor was suspended and
  resumed while producing it — no client retries, no substrate awareness.

The **agent never calls a substrate API.** All suspend/resume logic lives in
infrastructure components: an in-actor sidecar and two out-of-actor brokers.

## 2. Substrate facts this design relies on (verified in source)

Substrate is pre-alpha; pin the exact commit you build against.

1. **An actor is one gVisor sandbox** holding a `pause` container plus up to 10
   app containers (`pkg/api/v1alpha1/actortemplate_types.go`, `MaxItems=10`).
   All containers share one network namespace — one gVisor netstack, one
   `localhost` (`cmd/atelet/oci.go` gives every container the same netns path).
2. **Suspend = `runsc checkpoint` of the whole sandbox**; the user-space
   netstack is part of the checkpoint. The interior network config is a fixed
   constant on every worker (`eth0` = `169.254.17.2/30`,
   `cmd/ateom-gvisor/main.go setupActorNetwork`), so the frozen config stays
   valid after a cross-worker restore. **Consequence: loopback TCP connections
   between containers in the same actor survive suspend/resume, including
   resume on a different worker.** This is the foundation of the whole design
   (validated first — see §9).
3. **External TCP connections die on suspend, silently.** After checkpoint,
   ateom tears down the veth pair and NAT; egress was masqueraded behind the
   ephemeral worker-pod IP. `runsc` runs with `-allow-connected-on-save`, which
   only means checkpointing doesn't *fail* with connected sockets — on restore
   they are zombies (no FIN/RST is ever delivered). Only **outbound** dials the
   actor makes *after* it resumes can re-establish connectivity.
4. **Ingress routing + wake-on-request:** substrate's atenet runs stock Envoy
   with a Go ext_proc server that calls `Control.ResumeActor` (singleflight-
   deduped) for every inbound request, then routes to the actor's worker at
   `:80`. Substrate has no egress machinery — egress is a broad masquerade.
5. **Lifecycle API:** gRPC `Control` service (`pkg/proto/ateapipb`):
   `CreateActor`, `SuspendActor`, `ResumeActor` (idempotent fast-path when
   already RUNNING), `DeleteActor` (requires the actor be suspended first), etc.
6. **Actor identity** is a bind-mounted file `/run/ate/actor-id` (not env — env
   is frozen into the golden snapshot; see §10). Containers may declare an HTTP
   `readyz` probe in the ActorTemplate.

## 3. Architecture

```
                        ┌──────────── actor (gVisor sandbox) ────────────┐
 client                 │  egress-sidecar          agent-server (:8080)  │
   │                    │   ├─ ingress (:80) ──loopback──► /run          │
   ▼                    │   ├─ egress proxy (:15001) ◄─ HTTP(S)_PROXY    │
 ingress-broker         │   ├─ tunnel client(s)                          │
   │  /run  holds       │   └─ suspend poller ──► SuspendActor (ateapi)  │
   │        client conn └───────────┬───────────────────────────────────┘
   │  /reply ◄──── outbound ────────┘   tunnels re-dial + re-ATTACH on resume
   │        (survivable)                          │
   │                                              ▼
   └──── request via atenet (wakes actor) ─► egress-broker (Go)
                                              ├─ tunnel server
                                              ├─ per-session offset replay buffers
                                              ├─ holds MCP/LLM upstream open across suspend
                                              └─ wake policy ──► ResumeActor (ateapi)
                                                     │
                                                     ▼
                                              MCP server / Gemini (unaware)
```

### Division of authority

- **Sidecar owns suspend.** Co-located with the actor, it decides when to
  checkpoint (agent blocked on egress, or idle between requests) and calls
  `SuspendActor`.
- **Brokers own resume.** They observe the wake conditions the sidecar can't
  (a response arriving for a suspended actor; a client request arriving for one)
  and call `ResumeActor` — the egress-broker directly, the ingress-broker via
  substrate's atenet.

Neither side needs the other's cooperation.

### Awareness

| Component | Substrate awareness |
|---|---|
| client | none for suspend/resume — a plain HTTP caller. (It creates/deletes its actor as lifecycle bookkeeping; it never sees a suspend or resume.) |
| ADK agent | none — real destination URLs + standard `HTTP(S)_PROXY`/`NO_PROXY`; the activity plugin is generic introspection (no substrate imports) |
| MCP server / LLM | none — one connection, one request, one response; a suspended peer looks like a slow client (TCP flow control) or an idle keep-alive |
| egress-sidecar, egress-broker, ingress-broker | full (infrastructure) — read `/run/ate/actor-id`, call Suspend/Resume |

## 4. Egress interception: standard proxy env vars

The agent container gets:

```
HTTP_PROXY=http://127.0.0.1:15001
HTTPS_PROXY=http://127.0.0.1:15001
NO_PROXY=localhost,127.0.0.1
```

(The literal IP, not `localhost`: the gVisor actor image has no `/etc/hosts`,
so `localhost` would leak to cluster DNS and NXDOMAIN.)

Go's `net/http` honors these by default (`ProxyFromEnvironment`), covering the
ADK, the genai SDK, and the MCP Go SDK. The agent keeps its **real** destination
URLs; the sidecar learns each destination dynamically:

- `http://` destinations (the MCP server): the client sends the absolute-form
  request to the sidecar, which reads the target from the request line.
- `https://` destinations (Gemini): the client sends `CONNECT host:443`, then
  runs TLS **through** the tunnel to the real server. The sidecar and broker
  shuttle opaque bytes; the agent validates the real server certificate. The
  agent's TLS state is inside the checkpoint and the server's is on the socket
  the broker holds open, so the encrypted stream survives suspend/resume with
  no MITM.

**Untunneled connections and keep-alive pooling.** Any connection *not* routed
through the sidecar uses substrate's masquerade path and therefore dies on
suspend. HTTP clients pool keep-alive connections, so after a suspend the next
call on such a pool reuses a dead connection and fails (`connection reset by
peer`; Go won't auto-retry a POST on a broken persistent connection). Two ways
to live with a connection that must stay direct: tunnel it (then it survives),
or disable keep-alives so each call dials fresh after resume. In this PoC all of
the agent's egress is tunneled, so pooling is safe; the one place we disable
keep-alives is an *infrastructure* call — the agent-server's out-of-band reply
delivery on the ingress path (see §8), which fires right after a resume.

## 5. The resumable tunnel protocol (sidecar ⇄ egress-broker)

Resumption is at the **application byte-stream level with explicit offsets** —
not by reconstructing TCP state (which would require owning the netstack and is
fragile across gVisor restore).

One **session** = one agent-side egress connection (an `http://` request
pipeline or a `CONNECT` tunnel). Sessions are multiplexed over a single
sidecar→broker tunnel connection, re-dialed by the sidecar after every resume.

Frames (`internal/tunnel`):

| Frame | Direction | Purpose |
|---|---|---|
| `HELLO` | sidecar→broker | Opens a tunnel connection: "I speak for actor X" (X read fresh from `/run/ate/actor-id`). |
| `OPEN` | sidecar→broker | New session; broker dials the target (off the read loop, so a slow dial can't stall PINGs). |
| `ATTACH` | sidecar→broker | Re-adopt a session after resume: "I have delivered downstream up to offset N." |
| `ATTACH_OK` | broker→sidecar | Confirms attach: "I have written upstream up to offset M" — sidecar replays from there. |
| `DATA` | both | Payload at a cumulative per-direction offset. |
| `ACK` | both | Cumulative ack; sender trims its replay buffer. |
| `CLOSE` | both | Half-stream shutdown. |
| `PING`/`PONG` | both | Liveness / zombie-tunnel detection. |

Key mechanics:

- **Symmetric offset replay.** Each side keeps un-acked `DATA` for replay, so a
  suspend landing mid-request-body replays exactly like one landing
  mid-response. The sidecar's buffers are inside the checkpoint, so they can't
  be lost.
- **Backpressure.** The broker caps each session's buffer; when full it stops
  reading upstream, and TCP flow control makes the server see a slow client. No
  unbounded memory.
- **Zombie detection.** After restore the sidecar's old tunnel socket is a
  zombie (writes appear to succeed locally, nothing moves). Missed `PONG`s (~1 s
  ping interval, ~3 s timeout) trigger an immediate re-dial + `HELLO` + `ATTACH`
  for every live session.
- **Session lifetime ≠ request lifetime.** A session stays open across many
  request/response exchanges and idle gaps; it ends when the agent closes it,
  the upstream closes it, or broker GC reaps an orphan.

## 6. Suspend policy (sidecar)

The agent runs a generic **activity plugin** (`internal/activityz`) on the ADK
Plugin API: `Before/AfterRun` bracket invocations, `Before/AfterTool` bracket
tool calls, `Before/AfterModel` bracket LLM calls. It keeps in-flight counters
plus monotonic "started" counters and serves them at `GET 127.0.0.1:9902/statusz`.
It has no substrate imports — it is generic introspection any deployment could
use.

The sidecar suspends the actor in two situations:

1. **Blocked on egress.** A tool or model call has been in flight for
   > `BLOCKED_AFTER` (1 s) — the actor is provably waiting on egress the broker
   is holding. Model calls are included because LLM traffic is tunneled too.
2. **Idle between requests.** No client request has been in flight for
   > `IDLE_AFTER` (a few seconds). The signal is the **ingress interceptor's
   in-flight-request count** (§8): the sidecar owns the whole request→reply
   cycle, so `in-flight == 0` is ground truth that no client work is
   outstanding — precise and race-free (unlike a bare activity timer, which
   can't see the ingress side).

Correctness details, all learned on the cluster:

- **Suspend each tool/model call at most once.** After a suspend the broker
  holds the upstream and wakes the actor when the response arrives; on resume
  the *same* call is still in flight. Re-suspending then would loop
  (wake→re-suspend→wake) and the response would never reach the agent. The
  plugin exposes monotonic `ToolCallsStarted`/`ModelCallsStarted`; the sidecar
  records the value at each block-suspend and won't suspend again until a *new*
  call starts. Clock-independent — no timing grace needed.
- **Resume grace for idle-suspend.** The ingress idle timer is stale across a
  suspend. The poller detects a resume (a wall-clock gap between ticks far
  larger than the poll interval) and holds off idle-suspend for one idle window
  afterward, so the request that woke the actor can register as in-flight before
  the poller could re-suspend it.
- **`SuspendActor` from inside the actor never returns cleanly** — the caller
  freezes mid-RPC and the connection is a zombie on resume. It's fire-and-forget
  with a timeout; the error is expected on success.

The suspend policy is ADK-specific (via the plugin); the transport (§4–5) and
ingress (§8) are framework-agnostic. A natural future direction is for substrate
itself to poll a declared `idlez` probe (analogous to `readyz`) and own the
suspend decision — the sidecar's poll-and-suspend loop is a deliberate prototype
of that.

## 7. Wake policy (egress-broker)

Each session is **pending** (a request has gone upstream with no response yet)
or **quiescent** (its last request was fully answered — an idle keep-alive).

| Event on a *detached* session | Pending | Quiescent |
|---|---|---|
| Downstream data arrives | **ResumeActor** (the response to a waiting request) | buffer; **do not wake** |
| Upstream close / RST | **ResumeActor** (agent must see it and retry) | buffer; deliver on next attach; **do not wake** |

Waking only pending sessions is essential: idle keep-alive connections the
broker still holds for a suspended actor get routine housekeeping from the
servers (HTTP/2 PING/GOAWAY, TLS `close_notify`, idle-reap). Waking on that
would resurrect the actor endlessly (a RUNNING↔SUSPENDED oscillation). Quiescent
traffic is buffered and delivered on the next natural attach; the agent finds a
closed pooled connection and redials — standard client behavior.

Safety nets: wake-on-close for a pending session self-heals a suspend that
froze the agent mid-request (the server's read timeout eventually produces a
close → wake → the frozen write completes → normal error handling); a
max-suspend watchdog resumes any long-pending session as a backstop; and
`ResumeActor` is retried on `codes.Aborted` (the wake can race an in-progress
suspend) and singleflight-deduped per actor.

Debug endpoints for deterministic demos/tests: `POST /debug/suspend/{atespace}/{name}`,
`POST /debug/resume/{atespace}/{name}`, `GET /debug/sessions`.

## 8. Ingress: client-transparent reply-to relay

The client is a plain HTTP caller; the ingress-broker makes it unaware of
suspend/resume with a **reply-to callback**, not an app-specific
park/notify/dedup dance. The key insight: the request is small and delivered
before any suspend (suspend is triggered by the agent's *egress*), so what has
to survive is the **response** — which naturally wants to travel outbound, the
survivable direction.

```
1. client → ingress-broker /run  (holds the client connection)
2. broker → request via atenet → sidecar :80        (atenet wakes the actor;
     tagged X-Poc-Reply-To = broker pod addr, X-Poc-Request-Id = r)
3. sidecar acks 202 to atenet, forwards to agent :8080 over loopback
     (which survives the suspend/resume the agent does during its egress work)
4. sidecar delivers the response OUTBOUND to X-Poc-Reply-To, with retry
5. broker /reply matches X-Poc-Request-Id → the held client connection → writes
```

- **No offset byte-tunnel here.** For request/response, each leg is a whole HTTP
  message that can be retried atomically, so plain HTTP + an addressed callback
  suffices. (The offset tunnel is what a future *bidirectional-streaming*
  extension would use, moving the inbound leg onto a persistent sidecar-dialed
  tunnel to the same reply-to instance.)
- **The ingress-broker calls no substrate API** — atenet does the `ResumeActor`
  wake when the request is forwarded through it.
- **Scale-out for free.** The response is addressed to a specific broker
  instance (its pod IP), delivered outbound by the sidecar. So N ingress-broker
  replicas behind a plain L4 load balancer need **no rendezvous /
  consistent-hashing**: a client hits any instance, and the reply returns to
  that exact instance.
- **Header naming matters.** The reply-to headers are `X-Poc-*`, deliberately
  not `X-Request-Id` — that collides with Envoy's reserved `x-request-id`, which
  atenet mutates in transit (it packs a trace-sampling bit into a UUID nibble),
  corrupting the id.

The `/reply` delivery disables keep-alives and retries, because it fires right
after a resume when a connection pooled on a prior turn is dead.

## 9. Validation: loopback survival (the load-bearing assumption)

Everything rests on fact §2.2 — loopback TCP inside an actor surviving
suspend/resume — which the source supports but `-allow-connected-on-save` is
documented as a workaround for "a bug in networking resumption." So it's proven
empirically first, by `demos/loopback-survival`: two containers in one actor
hold an open loopback connection carrying sequenced, CRC'd, partially-acked
data; the actor is suspended and resumed (including forced onto a different
worker); the stream must continue byte-for-byte with zero sequence/CRC
violations. The same demo holds an *external* connection open through suspend to
characterize how the zombie socket manifests. Result: **PASS** across multiple
suspend/resume cycles, including cross-worker — the design is sound.

## 10. Hard-won lessons (things that bit us on the cluster)

- **Never cache anything actor-specific in memory.** Whatever an in-actor
  process reads once at startup is frozen into the ActorTemplate golden snapshot
  and inherited by *every* actor. The sidecar therefore reads
  `/run/ate/actor-id` **fresh** on every tunnel (re)connect and before every
  `SuspendActor` — an early version cached it at startup and every actor
  reported the golden actor's identity, so Suspend/Resume hit a deleted actor.
  (The atespace is constant per template, so it's fine to take from env.)
- **`HTTP_PROXY` must use `127.0.0.1`, not `localhost`** (no `/etc/hosts` in the
  actor).
- **The ActorTemplate `Container` schema has no `ports` field** — declaring one
  makes the template invalid. The sidecar just listens on `:80` in the shared
  netns; atenet's DNAT targets `:80` regardless.
- **ActorTemplate specs are immutable.** Changing an in-actor image or env
  requires deleting and recreating the template (which recuts the golden
  snapshot); the install script does this on `--deploy-demo-adk-calc`.
  Out-of-actor Deployments roll normally (fast `--redeploy-*` flags).
- **`x-request-id` collision** with Envoy (see §8).
- **Wake must be gated on "pending"** or idle keep-alive housekeeping oscillates
  the actor (see §7).
- **Idle-suspend needs a resume grace**, and each block-suspend must fire at
  most once per call, or a just-woken actor re-suspends before it can make
  progress (see §6).

## 11. Known limitations

- **Only proxy-honoring clients are protected.** A client that ignores
  `HTTP(S)_PROXY` (e.g. a raw gRPC client) falls through to the masquerade path
  and works but isn't suspend-transparent. Transparent capture (in-sandbox
  nftables REDIRECT) would close that gap.
- **Upstream max-request-duration limits** are orthogonal to substrate: a
  server that caps total request time will cut a very long tool call the same
  way it would for a never-suspended client. The error propagates to the agent's
  normal retry.
- **Bidirectional streaming** (long-lived client↔agent streams where the client
  keeps sending after a suspend) is not implemented; the reply-to design and the
  offset tunnel are the building blocks for it.
- **Substrate is pre-alpha** — pin the commit (and the runsc build its
  SandboxConfig references).
