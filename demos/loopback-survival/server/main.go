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

// Command server is the receiving half of the loopback-survival demo
// (phase 0 of substrate-poc-3, see DESIGN.md §9 R1).
//
// It runs as the second container of the actor, accepts a single loopback
// TCP connection from the client container, and verifies the framed stream:
// strictly increasing sequence numbers, per-frame CRCs, and a running CRC
// over all accepted payloads. Acks are delayed by --ack-delay so that under
// steady traffic there is almost always un-acked data in flight — a suspend
// landing at a random moment therefore checkpoints the connection with
// bytes buffered inside the gVisor netstack, which is exactly the state the
// experiment must prove survives restore.
//
// Stats are served as JSON on a loopback-only HTTP port; the client merges
// them into the actor's public status report on :80.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"github.com/dberkov/substrate-poc-3/demos/loopback-survival/internal/wire"
)

var (
	listenAddr      = pflag.String("listen", "127.0.0.1:7777", "loopback address to accept the client's data connection on")
	statsListenAddr = pflag.String("stats-listen", "127.0.0.1:7778", "loopback address for the JSON stats endpoint")
	ackDelay        = pflag.Duration("ack-delay", 300*time.Millisecond, "delay before acking a frame, to keep un-acked data in flight")
)

// stats is the server-side view of the experiment. Everything is
// cumulative across the process lifetime — which, under checkpoint/restore,
// includes all suspend/resume cycles.
type stats struct {
	mu sync.Mutex

	ConnectionsAccepted int    `json:"connectionsAccepted"`
	FramesReceived      uint64 `json:"framesReceived"`
	BytesReceived       uint64 `json:"bytesReceived"`
	LastSeqReceived     uint64 `json:"lastSeqReceived"`
	RunningCRC          uint32 `json:"runningCRC"`

	// Violations. All must remain zero for the experiment to pass.
	SeqViolations int `json:"seqViolations"`
	CRCViolations int `json:"crcViolations"`

	// LastConnError records how the previous data connection ended, if it
	// did. The client never redials after establishing, so anything but ""
	// (or the initial golden-snapshot handoff) is signal.
	LastConnError string `json:"lastConnError,omitempty"`
}

var st stats

func main() {
	pflag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	go serveStats(ctx)

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to listen", slog.String("addr", *listenAddr), slog.Any("err", err))
		os.Exit(1)
	}
	slog.InfoContext(ctx, "Loopback server listening", slog.String("addr", *listenAddr), slog.Duration("ackDelay", *ackDelay))

	// One connection at a time. The client dials exactly once and never
	// redials, so a second accept only ever happens if the client container
	// restarted — which the accept counter makes visible.
	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.ErrorContext(ctx, "Accept failed", slog.Any("err", err))
			os.Exit(1)
		}
		st.mu.Lock()
		st.ConnectionsAccepted++
		n := st.ConnectionsAccepted
		st.mu.Unlock()
		slog.InfoContext(ctx, "Accepted data connection", slog.Int("connection", n))
		handleConn(ctx, conn)
	}
}

// pendingAck carries a verified frame from the reader to the delayed-ack
// writer. Acks are written by a single goroutine in arrival order, so
// delaying them cannot reorder them.
type pendingAck struct {
	seq        uint64
	totalBytes uint64
	runningCRC uint32
	notBefore  time.Time
}

func handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	acks := make(chan pendingAck, 1024)
	done := make(chan struct{})

	go func() {
		defer close(done)
		w := bufio.NewWriter(conn)
		for ack := range acks {
			// Sleeping until notBefore preserves arrival order while holding
			// each ack back by --ack-delay. Under suspend the timer freezes
			// with everything else and continues after restore.
			if d := time.Until(ack.notBefore); d > 0 {
				time.Sleep(d)
			}
			if err := wire.WriteAck(w, ack.seq, ack.totalBytes, ack.runningCRC); err != nil {
				slog.ErrorContext(ctx, "Ack write failed", slog.Any("err", err))
				return
			}
			if err := w.Flush(); err != nil {
				slog.ErrorContext(ctx, "Ack flush failed", slog.Any("err", err))
				return
			}
		}
	}()

	r := bufio.NewReader(conn)
	for {
		seq, payload, err := wire.ReadFrame(r)
		if err != nil {
			st.mu.Lock()
			if errors.Is(err, wire.ErrCorrupt) {
				st.CRCViolations++
				st.mu.Unlock()
				slog.ErrorContext(ctx, "Frame CRC violation", slog.Uint64("seq", seq))
				continue
			}
			if !errors.Is(err, io.EOF) {
				st.LastConnError = err.Error()
			}
			st.mu.Unlock()
			slog.InfoContext(ctx, "Data connection closed", slog.Any("err", err))
			close(acks)
			<-done
			return
		}

		st.mu.Lock()
		if st.FramesReceived > 0 && seq != st.LastSeqReceived+1 {
			st.SeqViolations++
			slog.ErrorContext(ctx, "Sequence violation",
				slog.Uint64("expected", st.LastSeqReceived+1), slog.Uint64("got", seq))
		}
		st.LastSeqReceived = seq
		st.FramesReceived++
		st.BytesReceived += uint64(len(payload))
		st.RunningCRC = wire.UpdateCRC(st.RunningCRC, payload)
		ack := pendingAck{
			seq:        seq,
			totalBytes: st.BytesReceived,
			runningCRC: st.RunningCRC,
			notBefore:  time.Now().Add(*ackDelay),
		}
		st.mu.Unlock()

		select {
		case acks <- ack:
		default:
			// The ack writer is wedged; treat as a fatal experiment error
			// rather than blocking the reader silently.
			slog.ErrorContext(ctx, "Ack queue full; dropping connection")
			close(acks)
			<-done
			return
		}
	}
}

func serveStats(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/statz", func(w http.ResponseWriter, _ *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&st)
	})
	slog.InfoContext(ctx, "Stats server listening", slog.String("addr", *statsListenAddr))
	if err := http.ListenAndServe(*statsListenAddr, mux); err != nil {
		slog.ErrorContext(ctx, "Stats server failed", slog.Any("err", err))
		os.Exit(1)
	}
}
