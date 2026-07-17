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

// Package agent builds the calculator ADK agent. Compared with poc-1 this
// is a VANILLA agent: it has no substrate awareness whatsoever — no actor
// ID, no ateapi client, no self-suspend, no X-Actor-Id header. It talks to
// the MCP server at its REAL URL; the only reason its tool calls survive
// suspend/resume is the HTTP_PROXY env var pointing at the egress-sidecar,
// which Go's net/http honors automatically. This is the whole point of the
// PoC: the egress path is transparent.
package agent

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/adk/agent"
	"google.golang.org/adk/agent/llmagent"
	"google.golang.org/adk/model/gemini"
	"google.golang.org/adk/tool"
	"google.golang.org/adk/tool/mcptoolset"
	"google.golang.org/genai"
)

// Build constructs the calculator agent. It reads GOOGLE_API_KEY and
// CALC_MCP_URL from the environment. CALC_MCP_URL is the MCP server's real
// address; egress transparency comes entirely from HTTP_PROXY.
func Build(ctx context.Context) (agent.Agent, error) {
	apiKey := os.Getenv("GOOGLE_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("GOOGLE_API_KEY env var is required")
	}
	mcpURL := os.Getenv("CALC_MCP_URL")
	if mcpURL == "" {
		return nil, fmt.Errorf("CALC_MCP_URL env var is required")
	}

	// Phase-1 caveat: Gemini traffic is NOT tunneled (no HTTPS_PROXY set), so
	// it uses the direct masquerade egress path, and that external connection
	// dies on every actor suspend. The genai SDK pools keep-alive connections;
	// after a suspend during a tool call, the next LLM call would reuse a dead
	// pooled connection and fail with "connection reset by peer" (Go does not
	// auto-retry POSTs on a broken persistent connection). Disabling keep-
	// alives makes each LLM call dial fresh — the actor is resumed and
	// networked by the time it runs. Phase 2 removes this by routing Gemini
	// through the sidecar tunnel, where the connection survives suspend and
	// pooling is fine again.
	llmTransport := http.DefaultTransport.(*http.Transport).Clone()
	llmTransport.DisableKeepAlives = true
	model, err := gemini.NewModel(ctx, "gemini-flash-latest", &genai.ClientConfig{
		APIKey:     apiKey,
		HTTPClient: &http.Client{Transport: llmTransport},
	})
	if err != nil {
		return nil, fmt.Errorf("gemini.NewModel: %w", err)
	}

	mcpTools, err := mcptoolset.New(mcptoolset.Config{
		Transport: &mcp.StreamableClientTransport{
			Endpoint: mcpURL,
			// A stock HTTP client: its transport honors HTTP_PROXY/NO_PROXY
			// from the environment (http.ProxyFromEnvironment is the default),
			// so tool calls flow through the egress-sidecar with no
			// agent-side awareness. Nothing here mentions substrate.
			HTTPClient: &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}},
			// DisableStandaloneSSE keeps the client to a single POST /mcp per
			// call. This is an MCP-client choice (not substrate-related);
			// re-enabling the SSE stream over the tunnel is a phase-1.x
			// experiment (DESIGN.md §9 R5).
			DisableStandaloneSSE: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("mcptoolset.New: %w", err)
	}

	return llmagent.New(llmagent.Config{
		Name:        "calc_agent",
		Model:       model,
		Description: "Evaluates simple two-operand arithmetic expressions.",
		Instruction: "Parse the arithmetic expression in the user's message. " +
			"Always call the `calculator` tool with operands a, b and operator op (one of +, -, *, /). " +
			"After the tool returns, reply with ONLY the numeric result " +
			"(or the error string if the tool returned one). No prose, no explanation.",
		Toolsets: []tool.Toolset{mcpTools},
	})
}
