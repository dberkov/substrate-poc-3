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

// Command ingress-broker is the phase-3 client-transparent ingress relay.
// Clients hit /run on the client port; the agent's response is delivered
// back out-of-band to /run's caller via the sidecar posting to the reply
// port. See internal/ingressbroker. It uses no substrate API — atenet does
// the actor resume.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/dberkov/substrate-poc-3/demos/adk-calc/internal/ingressbroker"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	clientListen := flag.String("client-listen", envOr("CLIENT_LISTEN", ":80"), "client-facing address (/run)")
	replyListen := flag.String("reply-listen", envOr("REPLY_LISTEN", ":9090"), "sidecar-facing reply address (/reply)")
	atenet := flag.String("atenet", envOr("ATENET_ADDR", "atenet-router.ate-system.svc:80"), "atenet router address that fronts actors")
	atespace := flag.String("atespace", envOr("ATE_ATESPACE", "demo"), "atespace the actors live in")
	// The sidecar posts replies here; must be an address the actor can reach
	// directly (this pod's IP + reply port), so N replicas need no rendezvous.
	podIP := envOr("POD_IP", "")
	replyPort := envOr("REPLY_PORT", "9090")
	flag.Parse()

	replyAddr := envOr("REPLY_ADDR", "")
	if replyAddr == "" {
		if podIP == "" {
			log.Fatalf("REPLY_ADDR or POD_IP must be set so the sidecar can reach this instance")
		}
		replyAddr = fmt.Sprintf("%s:%s", podIP, replyPort)
	}

	b := ingressbroker.New(ingressbroker.Config{
		AtenetAddr: *atenet,
		Atespace:   *atespace,
		ReplyAddr:  replyAddr,
	})

	// Reply listener (sidecar → /reply), on its own port.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/reply", b.HandleReply)
		log.Printf("ingress-broker reply listening on %s (reply-to=%s)", *replyListen, replyAddr)
		log.Fatal(http.ListenAndServe(*replyListen, mux))
	}()

	// Client listener (client → /run).
	mux := http.NewServeMux()
	mux.HandleFunc("/run", b.HandleRun)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	log.Printf("ingress-broker client listening on %s (atenet=%s atespace=%s)", *clientListen, *atenet, *atespace)
	log.Fatal(http.ListenAndServe(*clientListen, mux))
}
