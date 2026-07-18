# adk-calc

Suspend/resume-transparent networking for a **vanilla** ADK agent, end to end
([DESIGN.md](../../DESIGN.md)). A plain HTTP client asks a calculator agent a
question; the agent calls an MCP tool and the LLM; the actor is suspended and
resumed while producing the answer — and the client, the agent, the MCP server,
and the LLM are all unaware it happened.

The agent has **no substrate awareness**: no actor ID, no `X-Actor-Id` header,
no self-suspend, no ateapi client. Its only egress configuration is standard
`HTTP_PROXY`/`HTTPS_PROXY` env vars pointing at the sidecar. The calculator
`calculator` tool sleeps 20 seconds on purpose, to force a long mid-call
suspend; any non-arithmetic question is answered directly by the LLM (which
exercises suspend during an HTTPS LLM call).

## Components

```
                          actor (gVisor sandbox)
   client ──▶ ingress-broker ──▶ atenet ──▶ ┌────────────────────────────┐
  (plain      (holds client conn,     wake-  │ agent-server (vanilla ADK) │
   HTTP)       reply-to callback)     on-req │  HTTP(S)_PROXY=127.0.0.1:… │
       ▲            ▲                         │  /statusz ◀── poll ──┐     │
       │  /reply ◀──┘  (outbound, survivable) │                      │     │
       └────────────────────────────────────┤ egress-sidecar ──────┘     │
                                             │  ingress :80 + egress proxy│
                                             │  + tunnel + suspend        │
                                             └──────────┬─────────────────┘
                                        tunnel (breaks  │  on suspend,
                                        + re-ATTACHes)   ▼  re-attaches
                                             ┌────────────────────────────┐
                                             │ egress-broker              │──▶ ateapi
                                             │ holds MCP+LLM conns open   │  ResumeActor
                                             │ across suspend; replays    │
                                             └──────────┬─────────────────┘
                                                        ▼
                                     mcp-server (20s calculator) · Gemini (HTTPS)
```

- **agent-server** — vanilla ADK agent + the generic `activityz` plugin
  (`/statusz`). In-actor. Both `HTTP_PROXY` (MCP) and `HTTPS_PROXY` (Gemini)
  route through the sidecar.
- **egress-sidecar** — in-actor. Egress forward proxy + ingress interceptor
  (`:80`) + resumable tunnel client + suspend poller. Owns suspend.
- **egress-broker** — out-of-actor. Holds the MCP and LLM connections open
  across the suspend and replays after the sidecar re-attaches. Owns resume.
- **mcp-server** — out-of-actor. The 20s calculator; fully substrate-unaware.
- **ingress-broker** — out-of-actor. Holds the client's connection and receives
  the response via an outbound reply-to callback from the sidecar (the client
  is fully transparent; see [DESIGN.md](../../DESIGN.md) §8).

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
calc> what is the weather like in New York?
Result: I don't have live weather data, but ...
```

Behind that single `Result: 7`, with the tool sleeping 20s:

1. The client POSTs `/run` to the ingress-broker, which holds the connection
   and forwards the request through atenet (waking the actor), tagged with a
   reply-to address.
2. The agent calls the `calculator` tool via `HTTP_PROXY` → sidecar → broker →
   mcp-server. The broker holds the MCP connection open.
3. ~1s in, the sidecar sees `toolBlockedMillis > 1s` on `/statusz` and calls
   `SuspendActor`. The actor is checkpointed; its tunnel to the broker dies.
4. 20s later mcp-server responds. The broker, detached and pending, calls
   `ResumeActor`.
5. The actor resumes (possibly on another worker); the sidecar re-dials the
   broker and `ATTACH`es; the broker replays the buffered response; the agent
   finishes the turn and the sidecar delivers the answer **outbound** to the
   ingress-broker's reply-to, which writes it back to the waiting client.

A non-arithmetic question follows the same path but suspends during the HTTPS
**LLM** call (`INCLUDE_MODEL_CALLS=true`), over a CONNECT tunnel — so one turn
may suspend and resume more than once (LLM call, then tool call). Each
tool/model call is suspended at most once, so resume delivers the response
rather than looping.

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

## Notes & limitations

- **Both MCP (HTTP) and Gemini (HTTPS) are tunneled** — the LLM connection
  rides a CONNECT tunnel and survives suspend/resume like the MCP path, with
  TLS end-to-end (the broker only shuttles opaque bytes; no MITM).
- MCP here is plain HTTP; Gemini already exercises the CONNECT/HTTPS path, so
  making our own MCP server HTTPS would add cert management for no extra proof.
  It's a trivial swap if desired (point `CALC_MCP_URL` at an `https://` MCP).
- `DisableStandaloneSSE` keeps the MCP client to one POST per call; SSE
  over the tunnel is a possible extension.
- The client is a substrate-*lifecycle* tool (it creates and deletes its own
  actor) but is fully transparent to suspend/resume — it never observes either.
