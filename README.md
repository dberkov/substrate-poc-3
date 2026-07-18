# substrate-poc-3

**Suspend/resume-transparent networking for AI agents on [Agent Substrate](https://github.com/agent-substrate/substrate).**

Agent Substrate multiplexes many "actors" (agents) onto a small pool of workers
by suspending idle actors (a gVisor checkpoint) and resuming them on demand —
possibly on a different worker pod. The catch: an actor's external TCP
connections don't survive that checkpoint. This PoC makes them survive
**invisibly**, so an ordinary ADK agent can be suspended and resumed *in the
middle of a request* and **nobody notices** — not the calling client, not the
agent, not the MCP server, not the LLM.

Concretely, it runs a Google ADK agent as a Substrate actor and keeps every
leg of its I/O transparent across suspend/resume:

- **Client → agent (ingress):** a plain HTTP client sends a request and gets
  its answer, even though the actor was suspended and resumed while producing
  it. The client makes no retries and needs no substrate knowledge.
- **Agent → MCP tool (egress):** a tool call that takes 20 s runs while the
  actor is suspended for ~19 of those seconds; the MCP server sees one normal
  request/response and never a disconnect.
- **Agent → LLM (egress):** the same, for HTTPS traffic to Gemini, over an
  end-to-end-encrypted tunnel (no MITM).

The agent binary is **vanilla** — its only suspend/resume-related configuration
is two standard `HTTP(S)_PROXY` environment variables. All the machinery lives
in infrastructure components beside and outside the actor.

See **[DESIGN.md](DESIGN.md)** for the architecture, the resumable tunnel
protocol, the suspend/wake policies, the decisions log, and the hard-won
lessons from running it on GKE.

## How it works, in one picture

```
                      ┌──────────── actor (gVisor sandbox) ─────────────┐
 client ─► ingress-   │  egress-sidecar ─► agent (:8080)                │
          broker ─────┼─► (:80) ▲            │ HTTP(S)_PROXY            │
             ▲        │         │ loopback   ▼                          │
             │        │         └──────── egress-sidecar (:15001) ──────┼─┐
             │        └──────────────────────────────────────────────── ┘ │ resumable
             │  reply (outbound, survives suspend)                         │ tunnel
             └──────────────────────── egress-broker ◄────────────────────┘ (re-attaches
                                            │  holds MCP/LLM upstreams open   on resume)
                                            ▼  across suspend, replays
                                     MCP server / Gemini (unaware)
```

- The **egress-sidecar** runs inside the actor next to the agent. Their
  connection is loopback, which lives inside the gVisor checkpoint and survives
  suspend/resume byte-for-byte. It proxies the agent's egress and, on resume,
  re-dials the brokers (outbound connections are the only kind an actor can
  re-establish after moving workers).
- The **egress-broker** (outside the actor) holds each upstream connection open
  across the suspend, buffers by byte offset, and replays after the sidecar
  re-attaches. It resumes the actor when a response arrives for a suspended one.
- The **ingress-broker** (outside the actor) makes the client transparent: it
  holds the client's connection, forwards the request via substrate's router
  (which wakes the actor), and the sidecar delivers the response back
  **outbound** — the direction that survives suspend — addressed to that broker
  instance.
- The **sidecar owns suspend** (it watches the agent's activity and its own
  in-flight request count); the **brokers own resume**. The agent never calls a
  substrate API.

## What it demonstrates

| Capability | Where |
|---|---|
| Loopback TCP inside an actor survives suspend/resume (the load-bearing assumption) | [`demos/loopback-survival`](demos/loopback-survival/) |
| Vanilla ADK agent; MCP tool egress transparent across a mid-call suspend | [`demos/adk-calc`](demos/adk-calc/) |
| LLM/HTTPS egress tunneled via CONNECT; suspend during LLM calls too | same demo |
| Client-transparent ingress; actor suspends between turns and wakes on demand | same demo |

The `adk-calc` demo is a calculator agent: arithmetic goes to an MCP
`calculator` tool (which sleeps 20 s, to exercise a long mid-call suspend), and
any other question is answered directly by the LLM.

## Repository layout

```
demos/loopback-survival/   Validation harness: loopback TCP across suspend/resume
demos/adk-calc/            The agent demo: agent, MCP server, ingress-broker, client
cmd/egress-broker/         Out-of-actor broker: holds upstreams, replays, resumes
cmd/egress-sidecar/        In-actor: egress proxy + ingress interceptor + tunnel + suspender
internal/tunnel/           Resumable byte-stream protocol (frames + offset replay buffer)
internal/broker/           Broker sessions, wake policy, resumer
internal/sidecar/          Sidecar proxy, ingress interceptor, tunnel client, suspend poller
internal/activityz/        Generic ADK activity plugin (/statusz)
internal/activitystatus/   Wire contract for the activity endpoint
internal/ateapi/           Thin substrate ateapi wrapper (suspend/resume/create/delete)
hack/install-poc.sh        Deploy dispatcher + per-demo install scripts
hack/poc-dev-env.sh.example  Copy to .poc-dev-env.sh and fill in
DESIGN.md                  The design document
```

The layout mirrors the Agent Substrate repo's own `demos/` + `hack/`
conventions, so the pieces can move upstream with minimal change.

## Quickstart

Prerequisites: a GKE cluster with Agent Substrate installed, plus `ko`, `jq`,
and the `kubectl-ate` plugin. A Gemini API key is needed for the agent demo.

```bash
cp hack/poc-dev-env.sh.example .poc-dev-env.sh
$EDITOR .poc-dev-env.sh        # PROJECT_ID, BUCKET_NAME, KO_DOCKER_REPO, GOOGLE_API_KEY

# Validation harness (no agent / no API key needed):
./hack/install-poc.sh --deploy-demo-loopback-survival

# The agent demo:
./hack/install-poc.sh --deploy-demo-adk-calc
```

Then follow the runbook in each demo's README:
[loopback-survival](demos/loopback-survival/README.md) ·
[adk-calc](demos/adk-calc/README.md).
