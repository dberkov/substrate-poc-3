# adk-calc (phase 1)

Suspend/resume-transparent egress for a **vanilla** ADK agent
([DESIGN.md](../../DESIGN.md) phase 1). The agent's MCP tool call survives the
actor being suspended and resumed mid-call, and the agent, the MCP server,
and the LLM are all unaware it happened.

This is the poc-1 calculator scenario — a `calculator` tool that sleeps 20
seconds — but with **no substrate awareness in the agent**: no actor ID, no
`X-Actor-Id` header, no self-suspend, no ateapi client. The only egress
configuration is a standard `HTTP_PROXY` env var pointing at the sidecar.

## Components

```
                          actor (gVisor sandbox)
   client ──▶ ingress-broker ──▶ atenet ──▶ ┌────────────────────────────┐
  (suspend-    (parks request        wake-  │ agent-server (vanilla ADK) │
   aware)       across suspend)      on-req │  HTTP_PROXY=127.0.0.1:15001│
       ▲            │                        │  /statusz ◀── poll ──┐     │
       └── /notify ─┘                        │                      │     │
                                             │ egress-sidecar ──────┘     │
                                             │  proxy + tunnel + suspend  │
                                             └──────────┬─────────────────┘
                                        tunnel (breaks  │  on suspend,
                                        + re-ATTACHes)   ▼  re-attaches)
                                             ┌────────────────────────────┐
                                             │ egress-broker              │──▶ ateapi
                                             │ holds MCP conn open across │  ResumeActor
                                             │ suspend; replays on attach │
                                             └──────────┬─────────────────┘
                                                        ▼
                                             mcp-server (20s calculator)
```

- **agent-server** — vanilla ADK agent + the generic `activityz` plugin
  (`/statusz`). In-actor. `HTTP_PROXY` routes its MCP calls through the
  sidecar; Gemini (HTTPS) has no `HTTPS_PROXY`, so it goes direct in phase 1.
- **egress-sidecar** — in-actor. Forward proxy + resumable tunnel client +
  suspend poller. Owns suspend.
- **egress-broker** — out-of-actor. Holds the MCP connection open across the
  suspend and replays the response after the sidecar re-attaches. Owns resume.
- **mcp-server** — out-of-actor. The 20s calculator; fully substrate-unaware.
- **ingress-broker** — out-of-actor. Parks the client's request across the
  suspend (phase-1 client stays suspend-aware; phase 3 removes this).

## Deploy

Prerequisites: a cluster with Agent Substrate installed, `ko`, `jq`, the
`kubectl-ate` plugin, and a Gemini API key.

```bash
cp hack/poc-dev-env.sh.example .poc-dev-env.sh
$EDITOR .poc-dev-env.sh    # PROJECT_ID, BUCKET_NAME, KO_DOCKER_REPO, GOOGLE_API_KEY

./hack/install-poc.sh --deploy-demo-adk-calc
```

## Run

Port-forward the ateapi (for the client's actor lifecycle) and the
ingress-broker (for /run):

```bash
# Terminal 1: ateapi gRPC
kubectl port-forward -n ate-system svc/api 8080:443
# Terminal 2: ingress-broker
kubectl port-forward -n ate-demo-adk-calc svc/ingress-broker 8000:80
```

Run the client (creates an actor in atespace `demo`, matching the template's
`ATE_ATESPACE`):

```bash
go run ./demos/adk-calc/client --ateapi=localhost:8080 --ingress=localhost:8000 --delete-on-exit=true
```

```
Session: 8f3a2c1e-...
calc> calculate 2+5=
Result: 7
```

Behind that single `Result: 7`, with the tool sleeping 20s:

1. The agent calls the `calculator` tool via `HTTP_PROXY` → sidecar → broker
   → mcp-server. The broker holds the MCP connection open.
2. ~1s in, the sidecar sees `toolBlockedMillis > 1s` on `/statusz` and calls
   `SuspendActor`. The actor is checkpointed; its tunnel to the broker dies.
3. 20s later mcp-server responds. The broker, detached and pending, calls
   `ResumeActor`.
4. The actor resumes (possibly on another worker); the sidecar re-dials the
   broker and `ATTACH`es; the broker replays the buffered response; the agent
   receives its tool result and finishes the turn.

## Observe the suspend/resume

```bash
# Watch the actor's status flip RUNNING → SUSPENDED → RUNNING during a call:
watch -n1 'kubectl ate get actors -a demo'

# Broker session table (upstream held, pending flag):
kubectl port-forward -n ate-demo-adk-calc svc/egress-broker 9001:9001 &
curl -s localhost:9001/debug/sessions | jq .

# Logs telling the story from each side:
kubectl logs -n ate-demo-adk-calc deploy/egress-broker            # "waking actor", "replayed downstream after attach"
kubectl ate logs actor <session-id> -a demo -c egress-sidecar     # "suspending actor", "tunnel connected ... reattach"
kubectl logs -n ate-demo-adk-calc deploy/mcp-server               # one request, one response — no disconnect
```

The mcp-server log is the proof of transparency: exactly one request and one
response per tool call, with no reconnect or retry, even though the actor was
suspended for most of the 20 seconds.

## Deterministic control

The broker exposes manual lifecycle endpoints for scripted demos/tests:

```bash
curl -X POST localhost:9001/debug/suspend/demo/<session-id>
curl -X POST localhost:9001/debug/resume/demo/<session-id>
```

## Iterating on the brokers

To rebuild and roll a single out-of-actor component without re-resolving the
whole template (which also rebuilds ateom-gvisor):

```bash
./hack/install-poc.sh --redeploy-egress-broker
./hack/install-poc.sh --redeploy-ingress-broker
./hack/install-poc.sh --redeploy-mcp-server
```

If the code hasn't changed (same image digest) the deployment is bounced via
`rollout restart` instead. Note that restarting the egress-broker drops its
in-memory sessions and held upstream connections — in-flight tool calls fail
and the agent retries; do it while nothing is mid-call.

In-actor images (agent-server, egress-sidecar) are baked into the
ActorTemplate golden snapshot, whose **spec is immutable** in substrate — a
plain `kubectl apply` of a changed template is rejected. So
`--deploy-demo-adk-calc` deletes the template (and its actors, which
reference it) and recreates it, cutting a fresh golden snapshot from the
current images. That means a full deploy destroys existing actors by design;
start a new client session afterwards.

> Gotcha worth knowing: an in-actor process must never cache anything
> actor-specific in memory (actor ID, etc.). Whatever it reads once at
> startup is frozen into the golden snapshot and inherited by every actor.
> The sidecar therefore re-reads `/run/ate/actor-id` fresh on every tunnel
> (re)connect and before every suspend, rather than at startup.

## Cleanup

```bash
./hack/install-poc.sh --delete-demo-adk-calc
```

## Notes & limitations (phase 1)

- Only HTTP MCP traffic is tunneled. Gemini (HTTPS) goes direct; suspend only
  fires on tool-call blocking, never during an LLM call, so the direct Gemini
  connection is never cut mid-request. Phase 2 tunnels HTTPS via CONNECT and
  enables model-call suspend.
- The client is suspend-aware (creates/deletes the actor, and its request is
  parked by the ingress-broker). Phase 3 makes the client transparent.
- `DisableStandaloneSSE` keeps the MCP client to one POST per call; re-enabling
  SSE-over-tunnel is the R5 experiment in DESIGN.md §9.
