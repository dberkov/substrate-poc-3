# Loopback Survival (phase 0)

The go/no-go experiment for substrate-poc-3 ([DESIGN.md](../../DESIGN.md) §9 R1):

> Does a loopback TCP connection between two containers in one actor survive
> substrate suspend/resume — including resume on a different worker pod —
> byte-for-byte?

The entire PoC (suspend/resume-transparent egress via an in-actor sidecar)
rests on the answer being yes. The substrate source strongly supports it
(the gVisor netstack is checkpointed; the interior network config is a fixed
constant on every pod), but `runsc` runs with `-allow-connected-on-save`,
documented as a workaround for "a bug in networking resumption" — so we
prove it empirically before writing any broker code.

## What runs where

One actor with two containers plus one regular Deployment outside substrate:

```
┌────────────── actor (gVisor sandbox) ──────────────┐
│  ┌──────────┐  sequenced, CRC'd     ┌────────────┐ │
│  │  client  │ ────────────────────▶ │   server   │ │
│  │          │ ◀──── delayed acks ── │            │ │
│  └────┬─────┘  127.0.0.1:7777       └────────────┘ │
│       │ status JSON on :80 (via atenet)            │
└────── │ ───────────────────────────────────────────┘
        │ external probe (expected to DIE on suspend)
        ▼
   externalecho Deployment/Service (plain k8s, outside substrate)
```

- **client → server (loopback):** a framed stream with strictly increasing
  sequence numbers, per-frame CRCs, and a running CRC over all payloads,
  cross-checked in acks. Acks are delayed 300ms so there is almost always
  un-acked data in flight when a suspend lands. The client dials **once**
  and never redials: any error after establishment is a permanent `FAIL`.
- **clock monitor:** wall-vs-monotonic clock sampling timestamps each
  suspend/resume boundary, so restores are visible in the report.
- **client → externalecho (external):** the control group. This connection
  is *expected* to die on every suspend (veth/NAT torn down, new pod IP).
  The probe records exactly how the zombie socket manifests after restore
  and how long detection takes — the number that calibrates the phase-1
  tunnel PING interval.
- The client's `/readyz` reports ready only after the loopback connection is
  established, so the ActorTemplate **golden snapshot already contains an
  open connection** — every actor created from the template begins by
  restoring one.

## Deploy

Prerequisites: a cluster with Agent Substrate installed
(`./hack/install-ate.sh --deploy-ate-system` in the substrate repo), `ko`,
`jq`, and the `kubectl-ate` plugin (`go install ./cmd/kubectl-ate` in the
substrate repo).

```bash
cp hack/poc-dev-env.sh.example .poc-dev-env.sh
$EDITOR .poc-dev-env.sh    # PROJECT_ID, BUCKET_NAME, KO_DOCKER_REPO

./hack/install-poc.sh --deploy-demo-loopback-survival
```

## Run the experiment

Create an actor and port-forward the atenet router:

```bash
kubectl ate create atespace demo || true
kubectl ate create actor ls-1 -a demo --template ate-demo-loopback-survival/loopback-survival
kubectl ate resume actor ls-1 -a demo

# separate terminal:
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

Query the report (any time; atenet wakes the actor if needed):

```bash
curl -s -H "Host: ls-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000/ | jq .
```

Let it stream for ~30s, then run suspend/resume cycles:

```bash
kubectl ate suspend actor ls-1 -a demo
sleep 30    # let the external side time out / observe the outage
kubectl ate resume actor ls-1 -a demo
curl -s -H "Host: ls-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000/ | jq .verdict,.verdictDetail,.restoresObserved
```

### Forcing a cross-worker resume

Resume normally lands on any free worker — possibly the one just vacated. To
force a different worker, occupy the vacated one with a filler actor first:

```bash
kubectl ate suspend actor ls-1 -a demo
kubectl ate create actor filler-1 -a demo --template ate-demo-loopback-survival/loopback-survival
kubectl ate resume actor filler-1 -a demo     # occupies a free worker
kubectl ate resume actor ls-1 -a demo         # lands elsewhere
kubectl ate get actors                        # confirm worker assignment changed
```

(The WorkerPool has 3 replicas; with one filler the odds are forced, with
two fillers it is guaranteed. Suspend and delete fillers afterwards.)

## Reading the report

`GET /` returns a JSON report with a computed `verdict`:

| Verdict | Meaning |
|---|---|
| `PASS` | ≥1 restore observed; loopback stream intact; zero seq/CRC/byte-count violations on both ends. **Phase 1 is a go.** |
| `NO_SUSPEND_OBSERVED` | Stream healthy but no suspend/resume boundary seen yet — suspend the actor and query again. |
| `FAIL` | The loopback connection broke or the stream was corrupted. `verdictDetail` says how. **Stop; re-evaluate the design.** |

Also inspect:

- `client.clockGaps` — one entry per detected restore (`wallGap` ≈ how long
  the actor was suspended; `monoGap` should stay ≈ the 100ms sample interval).
- `client.externalEvents` — the zombie-socket observations. For each
  post-restore failure note `kind` (`write-error` vs `read-error`: did the
  frozen socket swallow writes silently?) and `sinceLastOK` (detection
  latency with 1s probes and 2s deadlines). This calibrates the phase-1
  tunnel `PING` interval.
- `kubectl logs deploy/externalecho -n ate-demo-loopback-survival` — the
  outage as seen from the remote side (connection lifetimes, read timeouts).

## Success criteria (from DESIGN.md §9)

1. `PASS` across ≥5 suspend/resume cycles, including ≥1 forced cross-worker
   resume.
2. No violations with data in flight (the 300ms ack delay + 200ms send
   interval keep the window non-empty; suspends land at arbitrary points).
3. External probe events collected for ≥3 restores, with a documented
   zombie-detection profile (how failures manifest, detection latency).
4. The sandbox tolerates restoring with a zombie external socket — no
   crashes, no effect on the loopback stream.

## Cleanup

```bash
kubectl ate suspend actor ls-1 -a demo && kubectl ate delete actor ls-1 -a demo
./hack/install-poc.sh --delete-demo-loopback-survival
```
