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

// Command externalecho is a plain line-oriented TCP echo server for the
// loopback-survival demo. It runs as an ordinary Kubernetes Deployment —
// deliberately OUTSIDE any actor — and serves as the remote peer for the
// client's external probe: the connection class that is expected to die on
// every actor suspend. Its logs show, from the remote side, when the
// actor's connections go silent and when the probe redials after restore.
package main

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/spf13/pflag"
)

var listenAddr = pflag.String("listen", ":7070", "address to listen on")

func main() {
	pflag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to listen", slog.String("addr", *listenAddr), slog.Any("err", err))
		os.Exit(1)
	}
	slog.InfoContext(ctx, "External echo listening", slog.String("addr", *listenAddr))

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.ErrorContext(ctx, "Accept failed", slog.Any("err", err))
			os.Exit(1)
		}
		go handle(ctx, conn)
	}
}

func handle(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	start := time.Now()
	slog.InfoContext(ctx, "Connection opened", slog.String("remote", remote))
	defer func() {
		conn.Close()
		slog.InfoContext(ctx, "Connection closed",
			slog.String("remote", remote), slog.Duration("lifetime", time.Since(start)))
	}()

	r := bufio.NewReader(conn)
	var lines uint64
	for {
		// A generous idle deadline so half-open connections from suspended
		// actors are eventually reaped and logged, mimicking the idle
		// timeout of a typical upstream service.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		line, err := r.ReadString('\n')
		if err != nil {
			slog.InfoContext(ctx, "Read ended",
				slog.String("remote", remote), slog.Uint64("linesEchoed", lines), slog.Any("err", err))
			return
		}
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write([]byte(line)); err != nil {
			slog.InfoContext(ctx, "Write failed",
				slog.String("remote", remote), slog.Any("err", err))
			return
		}
		lines++
	}
}
