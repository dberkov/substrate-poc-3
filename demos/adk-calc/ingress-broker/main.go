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

// Command ingress-broker parks the client's /run request while the actor is
// suspended and re-issues it after the agent notifies on resume. In phase 1
// the client side is still suspend-aware (this broker exists); phase 3 makes
// it transparent. This is the egress PoC's ingress counterpart, ported from
// substrate-poc-1.
package main

import (
	"flag"
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
	listen := flag.String("listen", envOr("LISTEN_ADDR", ":80"), "address to listen on")
	atenet := flag.String("atenet", envOr("ATENET_ADDR", "atenet-router.ate-system.svc:80"), "atenet router address that fronts the actor")
	flag.Parse()

	b := ingressbroker.New(*atenet)
	mux := http.NewServeMux()
	mux.HandleFunc("/run", b.HandleRun)
	mux.HandleFunc("/notify", b.HandleNotify)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	log.Printf("ingress-broker listening on %s (atenet=%s)", *listen, *atenet)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
