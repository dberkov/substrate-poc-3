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

// Command client is the local dev driver for the adk-calc demo. It creates
// an actor (named after a fresh session ID), resumes it, and runs a REPL
// that sends each line to the agent through the ingress-broker. Unlike
// poc-1, it does NOT suspend the actor after each result — the egress-
// sidecar owns suspend now. On exit it suspends and (optionally) deletes the
// actor. The client is a dev tool and is deliberately substrate-aware; it is
// not part of the "unaware egress" claim.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/dberkov/substrate-poc-3/internal/ateapi"
)

type runReq struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
}

type runResp struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func main() {
	ateapiAddr := flag.String("ateapi", "localhost:8080", "ateapi gRPC address")
	ateapiInsecure := flag.Bool("ateapi-insecure", false, "use a plaintext ateapi connection")
	ingressAddr := flag.String("ingress", "localhost:8000", "ingress-broker HTTP address")
	atespace := flag.String("atespace", "demo", "atespace to create the actor in")
	templateNS := flag.String("template-namespace", "ate-demo-adk-calc", "ActorTemplate namespace")
	templateName := flag.String("template-name", "adk-calc", "ActorTemplate name")
	deleteOnExit := flag.Bool("delete-on-exit", false, "delete the actor after suspending on exit")
	flag.Parse()

	sessionID := uuid.NewString()
	actor := ateapi.Ref{Atespace: *atespace, Name: sessionID}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nInterrupt received, shutting down...")
		cancel()
	}()

	lc, err := ateapi.Dial(ateapi.Config{Addr: *ateapiAddr, Insecure: *ateapiInsecure})
	if err != nil {
		log.Fatalf("dial ateapi: %v", err)
	}
	defer lc.Close()

	log.Printf("Creating actor %s from template %s/%s...", actor, *templateNS, *templateName)
	if err := lc.CreateActor(ctx, actor, *templateNS, *templateName); err != nil {
		log.Fatalf("create actor: %v", err)
	}
	// No ResumeActor here: the ingress-broker's first request wakes the actor
	// via atenet. The client never drives suspend/resume — only create and
	// (suspend-then-)delete for lifecycle.

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		log.Printf("Suspending actor %s...", actor)
		if err := lc.SuspendActor(cleanupCtx, actor); err != nil {
			log.Printf("suspend actor: %v", err)
		}
		if *deleteOnExit {
			log.Printf("Deleting actor %s...", actor)
			if err := lc.DeleteActor(cleanupCtx, actor); err != nil {
				log.Printf("delete actor: %v", err)
			}
		}
	}()

	fmt.Printf("Session: %s\n", sessionID)
	fmt.Println("Type an arithmetic expression (e.g. 'calculate 2+5='). Type 'exit' to leave.")

	lines := make(chan string)
	scanErrors := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			scanErrors <- err
		}
		close(lines)
	}()

	for {
		fmt.Print("calc> ")
		select {
		case <-ctx.Done():
			return
		case err := <-scanErrors:
			log.Printf("read stdin: %v", err)
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if line == "exit" {
				return
			}
			result, err := callAgent(ctx, *ingressAddr, sessionID, line)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			if result.Error != "" {
				fmt.Printf("Agent error: %s\n", result.Error)
				continue
			}
			fmt.Printf("Result: %s\n", result.Result)
		}
	}
}

func callAgent(ctx context.Context, ingressAddr, sessionID, input string) (*runResp, error) {
	url := fmt.Sprintf("http://%s/run", ingressAddr)
	body, _ := json.Marshal(runReq{SessionID: sessionID, Input: input})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Generous timeout: the ingress-broker holds the call across the actor's
	// suspend/resume. Ctrl-C cancels via ctx.
	httpClient := &http.Client{Timeout: 10 * time.Minute}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(raw))
	}
	var parsed runResp
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	return &parsed, nil
}
