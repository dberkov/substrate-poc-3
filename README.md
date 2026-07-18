# substrate-poc-3

Suspend/resume-transparent egress for [Agent Substrate](https://github.com/agent-substrate/substrate):
an ADK agent whose outgoing traffic (MCP tools, LLM APIs) is **completely
unaware** that its actor is being suspended, snapshotted, and resumed —
possibly on a different worker pod — mid-request.

See **[DESIGN.md](DESIGN.md)** for the full concept: architecture, the
resumable tunnel protocol, suspend/wake policies, decisions log, and risks.
The short version:

- An **egress-sidecar** container runs in the same actor as the agent. The
  agent reaches it via plain `HTTP(S)_PROXY` env vars; their loopback
  connection lives inside the checkpointed gVisor netstack and survives
  suspend/resume.
- A **Go egress-broker** outside the actor holds the upstream connection
  open across suspends, buffers with byte offsets, and replays after the
  sidecar re-attaches. It calls `ResumeActor` when response data arrives
  for a suspended actor.
- The sidecar owns suspend (polling a generic ADK activity plugin); the
  broker owns resume. The agent, the MCP server, and the LLM know nothing.

## Repository layout

The layout deliberately mirrors the substrate repo (`demos/<name>/` with a
`<name>.yaml.tmpl`, sourced `hack/install-demo-<name>.sh` scripts registered
in a dispatcher) so the PoC can migrate into that project with minimal
changes.

```
demos/loopback-survival/   Phase 0: the go/no-go experiment
demos/adk-calc/            Phase 1: vanilla ADK agent, transparent MCP egress
cmd/egress-broker/         Phase 1: out-of-actor broker (holds upstream, resumes)
cmd/egress-sidecar/        Phase 1: in-actor proxy + tunnel client + suspender
internal/tunnel/           Resumable byte-stream protocol (frames + replay buffer)
internal/broker/           Broker sessions, wake policy, resumer
internal/sidecar/          Sidecar proxy, tunnel client, suspend poller
internal/activityz/        Generic ADK activity plugin (/statusz)
internal/ateapi/           Thin substrate ateapi wrapper (suspend/resume)
hack/install-poc.sh        Deploy dispatcher (modeled on substrate's install-ate.sh)
hack/poc-dev-env.sh.example  Copy to .poc-dev-env.sh and fill in
DESIGN.md                  The design document
```

## Phases

| Phase | Deliverable | Status |
|---|---|---|
| 0 | [`demos/loopback-survival`](demos/loopback-survival/) — prove loopback TCP survives suspend/resume | **PASS** (2026-07-16: 3 suspend/resume cycles, zero violations) |
| 1 | [`demos/adk-calc`](demos/adk-calc/) — tunnel + sidecar + broker; vanilla ADK agent, HTTP MCP | **done** (committed on `phase-1`) |
| 2 | LLM (Gemini/HTTPS) tunneled via CONNECT; suspend during LLM calls too | **done** (on `main`) |
| 3 | Transparent ingress — client unaware of suspend/resume (reply-to relay) | **implemented, needs cluster run** |
| 4 | Envoy composition for egress policy/observability (optional) | — |

## Quickstart (phase 0)

Prerequisites: GKE cluster with Agent Substrate deployed, `ko`, `jq`,
`kubectl-ate` plugin.

```bash
cp hack/poc-dev-env.sh.example .poc-dev-env.sh
$EDITOR .poc-dev-env.sh

./hack/install-poc.sh --deploy-demo-loopback-survival
```

Then follow the runbook in
[demos/loopback-survival/README.md](demos/loopback-survival/README.md).
