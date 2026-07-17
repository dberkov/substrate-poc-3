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

// Command mcp-server is the calculator MCP server: a plain HTTP service,
// deployed OUTSIDE substrate as an ordinary Deployment. The calculator tool
// sleeps 20 seconds, so the actor is suspended for most of every tool call —
// yet this server is completely unaware of substrate: it sees one
// connection, one request, and sends one response to a peer (the broker)
// that never disconnects.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type calcArgs struct {
	A  float64 `json:"a"  jsonschema:"First operand"`
	B  float64 `json:"b"  jsonschema:"Second operand"`
	Op string  `json:"op" jsonschema:"Operator, one of +, -, *, /"`
}

type calcResult struct {
	Value float64 `json:"value,omitempty"`
	Error string  `json:"error,omitempty"`
}

func calculate(_ context.Context, _ *mcp.CallToolRequest, in calcArgs) (_ *mcp.CallToolResult, res calcResult, _ error) {
	defer func() { log.Printf("calculator: result=%v", res) }()
	log.Printf("calculator: received a=%v b=%v op=%q; sleeping 20s", in.A, in.B, in.Op)
	time.Sleep(20 * time.Second)
	switch in.Op {
	case "+":
		return nil, calcResult{Value: in.A + in.B}, nil
	case "-":
		return nil, calcResult{Value: in.A - in.B}, nil
	case "*":
		return nil, calcResult{Value: in.A * in.B}, nil
	case "/":
		if in.B == 0 {
			return nil, calcResult{Error: "division by zero"}, nil
		}
		return nil, calcResult{Value: in.A / in.B}, nil
	default:
		return nil, calcResult{Error: fmt.Sprintf("unsupported operator %q", in.Op)}, nil
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	listen := flag.String("listen", envOr("LISTEN_ADDR", ":80"), "address to listen on")
	flag.Parse()

	server := mcp.NewServer(&mcp.Implementation{Name: "calc-mcp-server", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "calculator",
		Description: "Computes A op B for + - * /. Takes ~20 seconds.",
	}, calculate)

	// Stateless single-shot JSON responses: each POST /mcp is self-contained,
	// which is ideal for the opaque byte tunnel (no server-side session state
	// to lose across a suspend).
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})

	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.Handle("/mcp/", handler)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	log.Printf("mcp-server listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
