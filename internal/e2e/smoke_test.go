package e2e

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/dberkov/substrate-poc-3/internal/broker"
	"github.com/dberkov/substrate-poc-3/internal/sidecar"
)

// TestProxyLargeChunkedResponse streams a large, flushed, multi-chunk
// response through the real Proxy + tunnel + broker against a real HTTP
// upstream, asserting byte-exact delivery.
func TestProxyLargeChunkedResponse(t *testing.T) {
	want := bytes.Repeat([]byte("Z"), 500000)
	upLn, _ := net.Listen("tcp", "127.0.0.1:0")
	mux := http.NewServeMux()
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "500000")
		w.Write(want[:250000])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(300 * time.Millisecond)
		w.Write(want[250000:])
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(upLn)
	defer srv.Close()

	lc := newFakeLC()
	brokerAddr := startBroker(t, lc)

	client := sidecar.NewClient(sidecar.ClientConfig{
		ActorID: func() string { return "demo/a" }, BrokerAddr: brokerAddr,
		PingInterval: 100 * time.Millisecond, PongTimeout: 500 * time.Millisecond,
		DialBackoff: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.Run(ctx)

	proxyLn, _ := net.Listen("tcp", "127.0.0.1:0")
	go sidecar.NewProxy(client, nil).Serve(ctx, proxyLn)
	time.Sleep(200 * time.Millisecond)

	pu, _ := url.Parse("http://" + proxyLn.Addr().String())
	hc := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(pu)}, Timeout: 15 * time.Second}
	resp, err := hc.Get("http://" + upLn.Addr().String() + "/big")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("body mismatch: got %d want %d", len(body), len(want))
	}
	_ = broker.SessionInfo{}
}
