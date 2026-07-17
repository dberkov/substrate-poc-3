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

// Command client is the sending half of the loopback-survival demo
// (phase 0 of substrate-poc-3, see DESIGN.md §9 R1). It answers the
// go/no-go question the whole PoC rests on:
//
//	Does a loopback TCP connection between two containers in one actor
//	survive substrate suspend/resume — including resume on a different
//	worker pod — byte-for-byte?
//
// It runs three independent instruments and publishes their combined
// verdict as JSON on :80 (reachable through the atenet router):
//
//  1. Loopback stream: dials the server container over 127.0.0.1 exactly
//     once, then sends CRC'd, sequenced frames forever. It NEVER redials —
//     any error on this connection after establishment is a permanent
//     experiment failure, not something to paper over with a reconnect.
//  2. Clock monitor: samples wall vs monotonic time; a wall-clock jump
//     without a matching monotonic jump marks a suspend/resume boundary,
//     giving each restore a timestamp to correlate with the other two
//     instruments.
//  3. External probe: holds a TCP connection to a plain in-cluster echo
//     service OUTSIDE the actor. This connection is expected to die on
//     every suspend (the veth/NAT path is torn down); the probe records
//     exactly how the zombie socket manifests after restore and how long
//     detection takes — the number that calibrates the egress-sidecar's
//     tunnel PING interval in phase 1.
//
// The /readyz endpoint reports ready only after the loopback connection is
// established, so the ActorTemplate golden snapshot is taken with the
// connection already open: every actor created from the template starts
// life restoring an established connection, which is itself part of the
// test.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"github.com/dberkov/substrate-poc-3/demos/loopback-survival/internal/wire"
)

var (
	serverAddr      = pflag.String("server-addr", "127.0.0.1:7777", "loopback address of the server container's data listener")
	serverStatsURL  = pflag.String("server-stats-url", "http://127.0.0.1:7778/statz", "URL of the server container's stats endpoint")
	httpListenAddr  = pflag.String("http-listen", ":80", "address of the public status/readyz endpoint (atenet routes to :80)")
	sendInterval    = pflag.Duration("send-interval", 200*time.Millisecond, "interval between data frames")
	payloadSize     = pflag.Int("payload-size", 1024, "data frame payload size in bytes")
	externalAddr    = pflag.String("external-addr", "", "host:port of the external echo service; empty disables the external probe")
	clockSampleInt  = pflag.Duration("clock-sample-interval", 100*time.Millisecond, "clock monitor sampling interval")
	clockGapMin     = pflag.Duration("clock-gap-min", 2*time.Second, "minimum wall-clock jump to record as a suspend/resume boundary")
	externalTimeout = pflag.Duration("external-timeout", 2*time.Second, "read/write deadline for the external probe")
)

const maxEvents = 200

// clockGap records one detected suspend/resume boundary.
type clockGap struct {
	DetectedAt time.Time     `json:"detectedAt"`
	WallGap    time.Duration `json:"wallGap"`
	MonoGap    time.Duration `json:"monoGap"`
}

// extEvent records one observation on the external probe connection —
// established, failed (with the phase and error), or reconnected.
type extEvent struct {
	At    time.Time `json:"at"`
	Kind  string    `json:"kind"` // "connected", "write-error", "read-error", "corrupt-echo"
	Probe uint64    `json:"probe,omitempty"`
	Error string    `json:"error,omitempty"`
	// SinceLastOK is how long the connection appeared healthy before this
	// failure was detected — after a restore this measures zombie-socket
	// detection latency.
	SinceLastOK time.Duration `json:"sinceLastOK,omitempty"`
}

type stats struct {
	mu sync.Mutex

	StartedAt time.Time `json:"startedAt"`

	// Loopback stream.
	LoopbackEstablishedAt time.Time `json:"loopbackEstablishedAt,omitzero"`
	FramesSent            uint64    `json:"framesSent"`
	BytesSent             uint64    `json:"bytesSent"`
	LastSeqSent           uint64    `json:"lastSeqSent"`
	AcksReceived          uint64    `json:"acksReceived"`
	LastSeqAcked          uint64    `json:"lastSeqAcked"`
	SendRunningCRC        uint32    `json:"sendRunningCRC"`
	AckOrderViolations    int       `json:"ackOrderViolations"`
	AckCRCMismatches      int       `json:"ackCRCMismatches"`
	AckBytesMismatches    int       `json:"ackBytesMismatches"`
	LoopbackBroken        bool      `json:"loopbackBroken"`
	LoopbackError         string    `json:"loopbackError,omitempty"`

	// pendingCRC maps an un-acked seq to the (runningCRC, totalBytes) the
	// server must report when acking it. Bounded by the in-flight window.
	pendingCRC map[uint64][2]uint64

	// Clock monitor.
	ClockGaps []clockGap `json:"clockGaps"`

	// External probe.
	ExternalProbesSent  uint64     `json:"externalProbesSent"`
	ExternalEchoesOK    uint64     `json:"externalEchoesOK"`
	ExternalReconnects  int        `json:"externalReconnects"`
	ExternalEvents      []extEvent `json:"externalEvents"`
	externalLastOK      time.Time
	externalLastOKValid bool
}

var (
	st    = stats{StartedAt: time.Now(), pendingCRC: make(map[uint64][2]uint64)}
	ready atomic.Bool
)

func main() {
	pflag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	go serveHTTP(ctx)
	go monitorClocks(ctx)
	if *externalAddr != "" {
		go runExternalProbe(ctx)
	} else {
		slog.InfoContext(ctx, "External probe disabled (--external-addr empty)")
	}

	runLoopbackStream(ctx)

	// The loopback stream only returns on permanent failure. Keep serving
	// the status report so the failure is inspectable through atenet.
	slog.ErrorContext(ctx, "Loopback stream terminated; status endpoint stays up for inspection")
	select {}
}

// --- Instrument 1: the loopback stream ---

func runLoopbackStream(ctx context.Context) {
	// Retry the initial dial only: the server container may still be
	// starting. Once established, there are no second chances.
	var conn net.Conn
	for {
		var err error
		conn, err = net.Dial("tcp", *serverAddr)
		if err == nil {
			break
		}
		slog.InfoContext(ctx, "Waiting for server container", slog.Any("err", err))
		time.Sleep(500 * time.Millisecond)
	}
	st.mu.Lock()
	st.LoopbackEstablishedAt = time.Now()
	st.mu.Unlock()
	ready.Store(true)
	slog.InfoContext(ctx, "Loopback connection established; readyz now reports OK",
		slog.String("addr", *serverAddr))

	go readAcks(ctx, conn)

	w := bufio.NewWriter(conn)
	payload := make([]byte, *payloadSize)
	seq := uint64(0)
	for range time.Tick(*sendInterval) {
		seq++
		wire.FillPayload(payload, seq)

		st.mu.Lock()
		st.SendRunningCRC = wire.UpdateCRC(st.SendRunningCRC, payload)
		st.BytesSent += uint64(len(payload))
		st.pendingCRC[seq] = [2]uint64{uint64(st.SendRunningCRC), st.BytesSent}
		st.FramesSent++
		st.LastSeqSent = seq
		st.mu.Unlock()

		if err := wire.WriteFrame(w, seq, payload); err != nil {
			failLoopback(ctx, "write", err)
			return
		}
		if err := w.Flush(); err != nil {
			failLoopback(ctx, "flush", err)
			return
		}
	}
}

func readAcks(ctx context.Context, conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		seq, totalBytes, runningCRC, err := wire.ReadAck(r)
		if err != nil {
			failLoopback(ctx, "read-ack", err)
			return
		}
		st.mu.Lock()
		st.AcksReceived++
		if st.LastSeqAcked != 0 && seq != st.LastSeqAcked+1 {
			st.AckOrderViolations++
			slog.ErrorContext(ctx, "Ack order violation",
				slog.Uint64("expected", st.LastSeqAcked+1), slog.Uint64("got", seq))
		}
		st.LastSeqAcked = seq
		if want, ok := st.pendingCRC[seq]; ok {
			if uint32(want[0]) != runningCRC {
				st.AckCRCMismatches++
				slog.ErrorContext(ctx, "Running CRC diverged",
					slog.Uint64("seq", seq),
					slog.Uint64("want", want[0]), slog.Uint64("got", uint64(runningCRC)))
			}
			if want[1] != totalBytes {
				st.AckBytesMismatches++
				slog.ErrorContext(ctx, "Total byte count diverged",
					slog.Uint64("seq", seq),
					slog.Uint64("want", want[1]), slog.Uint64("got", totalBytes))
			}
			delete(st.pendingCRC, seq)
		}
		st.mu.Unlock()
	}
}

func failLoopback(ctx context.Context, phase string, err error) {
	st.mu.Lock()
	if !st.LoopbackBroken {
		st.LoopbackBroken = true
		st.LoopbackError = fmt.Sprintf("%s: %v", phase, err)
	}
	st.mu.Unlock()
	slog.ErrorContext(ctx, "LOOPBACK CONNECTION FAILED — experiment verdict is FAIL",
		slog.String("phase", phase), slog.Any("err", err))
}

// --- Instrument 2: the clock monitor ---

// monitorClocks detects suspend/resume boundaries. time.Now() carries both
// a wall and a monotonic reading: Sub uses the monotonic parts, while
// Round(0) strips them so Sub compares wall clocks. Across a
// checkpoint/restore the wall clock jumps by the suspension length; the
// sandbox-internal monotonic clock does not advance while checkpointed.
func monitorClocks(ctx context.Context) {
	prev := time.Now()
	for range time.Tick(*clockSampleInt) {
		now := time.Now()
		monoGap := now.Sub(prev)
		wallGap := now.Round(0).Sub(prev.Round(0))
		if wallGap-monoGap > *clockGapMin || monoGap > *clockGapMin+*clockSampleInt {
			gap := clockGap{DetectedAt: now, WallGap: wallGap, MonoGap: monoGap}
			st.mu.Lock()
			if len(st.ClockGaps) < maxEvents {
				st.ClockGaps = append(st.ClockGaps, gap)
			}
			st.mu.Unlock()
			slog.InfoContext(ctx, "Clock gap detected (suspend/resume boundary)",
				slog.Duration("wallGap", wallGap), slog.Duration("monoGap", monoGap))
		}
		prev = now
	}
}

// --- Instrument 3: the external probe ---

// runExternalProbe holds a TCP connection to the external echo service and
// records how it dies across each suspend. Unlike the loopback stream,
// reconnecting here is expected and correct — this is the connection class
// the phase-1 sidecar↔broker tunnel will own.
func runExternalProbe(ctx context.Context) {
	var probe uint64
	for {
		conn, err := net.DialTimeout("tcp", *externalAddr, 5*time.Second)
		if err != nil {
			slog.InfoContext(ctx, "External dial failed; retrying", slog.Any("err", err))
			time.Sleep(time.Second)
			continue
		}
		recordExtEvent(ctx, extEvent{At: time.Now(), Kind: "connected"})
		r := bufio.NewReader(conn)

		for {
			probe++
			line := fmt.Sprintf("probe %d %d\n", probe, time.Now().UnixNano())

			st.mu.Lock()
			st.ExternalProbesSent++
			st.mu.Unlock()

			_ = conn.SetWriteDeadline(time.Now().Add(*externalTimeout))
			if _, err := conn.Write([]byte(line)); err != nil {
				recordExtFailure(ctx, "write-error", probe, err)
				break
			}
			_ = conn.SetReadDeadline(time.Now().Add(*externalTimeout))
			echo, err := r.ReadString('\n')
			if err != nil {
				recordExtFailure(ctx, "read-error", probe, err)
				break
			}
			if echo != line {
				recordExtFailure(ctx, "corrupt-echo", probe, fmt.Errorf("sent %q got %q", line, echo))
				break
			}

			st.mu.Lock()
			st.ExternalEchoesOK++
			st.externalLastOK = time.Now()
			st.externalLastOKValid = true
			st.mu.Unlock()

			time.Sleep(time.Second)
		}
		conn.Close()

		st.mu.Lock()
		st.ExternalReconnects++
		st.mu.Unlock()
	}
}

func recordExtFailure(ctx context.Context, kind string, probe uint64, err error) {
	ev := extEvent{At: time.Now(), Kind: kind, Probe: probe, Error: err.Error()}
	st.mu.Lock()
	if st.externalLastOKValid {
		ev.SinceLastOK = time.Since(st.externalLastOK)
	}
	st.mu.Unlock()
	recordExtEvent(ctx, ev)
}

func recordExtEvent(ctx context.Context, ev extEvent) {
	st.mu.Lock()
	if len(st.ExternalEvents) < maxEvents {
		st.ExternalEvents = append(st.ExternalEvents, ev)
	}
	st.mu.Unlock()
	slog.InfoContext(ctx, "External probe event",
		slog.String("kind", ev.Kind), slog.String("err", ev.Error),
		slog.Duration("sinceLastOK", ev.SinceLastOK))
}

// --- The public status report ---

// report is what GET / returns: both sides' raw stats plus a computed
// verdict, so a runbook (or a human with curl) needs no further tooling.
type report struct {
	Verdict          string          `json:"verdict"` // PASS | FAIL | NO_SUSPEND_OBSERVED
	VerdictDetail    []string        `json:"verdictDetail"`
	RestoresObserved int             `json:"restoresObserved"`
	Client           json.RawMessage `json:"client"`
	Server           json.RawMessage `json:"server,omitempty"`
	ServerStatsErr   string          `json:"serverStatsError,omitempty"`
}

func serveHTTP(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		rep := buildReport(r.Context())
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	})
	// /readyz gates the ActorTemplate golden snapshot: ready only once the
	// loopback connection is established, so every snapshot — golden and
	// per-suspend — contains an open connection.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "loopback connection not yet established", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	slog.InfoContext(ctx, "Status server listening", slog.String("addr", *httpListenAddr))
	if err := http.ListenAndServe(*httpListenAddr, mux); err != nil {
		slog.ErrorContext(ctx, "Status server failed", slog.Any("err", err))
		os.Exit(1)
	}
}

func buildReport(ctx context.Context) report {
	var rep report

	st.mu.Lock()
	clientJSON, _ := json.Marshal(&st)
	broken := st.LoopbackBroken
	established := !st.LoopbackEstablishedAt.IsZero()
	violations := st.AckOrderViolations + st.AckCRCMismatches + st.AckBytesMismatches
	restores := len(st.ClockGaps)
	lastSent, lastAcked := st.LastSeqSent, st.LastSeqAcked
	st.mu.Unlock()
	rep.Client = clientJSON
	rep.RestoresObserved = restores

	// Merge the server container's stats over loopback HTTP.
	var srvSeqViolations, srvCRCViolations int
	reqCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, *serverStatsURL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		rep.ServerStatsErr = err.Error()
	} else {
		defer resp.Body.Close()
		var srv struct {
			SeqViolations int `json:"seqViolations"`
			CRCViolations int `json:"crcViolations"`
		}
		raw := json.RawMessage{}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			rep.ServerStatsErr = err.Error()
		} else {
			rep.Server = raw
			if err := json.Unmarshal(raw, &srv); err == nil {
				srvSeqViolations, srvCRCViolations = srv.SeqViolations, srv.CRCViolations
			}
		}
	}

	fail := func(msg string) {
		rep.Verdict = "FAIL"
		rep.VerdictDetail = append(rep.VerdictDetail, msg)
	}
	switch {
	case !established:
		fail("loopback connection never established")
	case broken:
		fail("loopback connection broke after establishment")
	}
	if violations > 0 {
		fail(fmt.Sprintf("%d client-side ack/CRC/byte-count violations", violations))
	}
	if srvSeqViolations+srvCRCViolations > 0 {
		fail(fmt.Sprintf("server reported %d seq and %d CRC violations", srvSeqViolations, srvCRCViolations))
	}
	if rep.Verdict == "" {
		if restores == 0 {
			rep.Verdict = "NO_SUSPEND_OBSERVED"
			rep.VerdictDetail = append(rep.VerdictDetail,
				"stream healthy, but no suspend/resume boundary detected yet — suspend the actor and query again")
		} else {
			rep.Verdict = "PASS"
			rep.VerdictDetail = append(rep.VerdictDetail, fmt.Sprintf(
				"loopback stream intact across %d restore(s); %d frames sent, %d acked, zero violations",
				restores, lastSent, lastAcked))
		}
	}
	return rep
}
