// Package ingressbroker is an HTTP-to-HTTP proxy that survives an
// actor-substrate suspend/resume cycle. It parks the client request while
// the upstream actor is unreachable, then re-issues the upstream call after
// the agent POSTs to /notify on resume. The replay relies on the agent's
// dedup cache (see internal/agentsrv) returning the same answer for an
// identical (sessionID, input) pair.
package ingressbroker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

const actorHostSuffix = ".demo.actors.resources.substrate.ate.dev"

type Broker struct {
	atenetAddr string
	httpClient *http.Client
	waiters    *waiterTable
}

func New(atenetAddr string) *Broker {
	return &Broker{
		atenetAddr: atenetAddr,
		// No client-side timeout: the broker holds the call across an actor
		// suspend/resume. Cancellation flows through the request context.
		httpClient: &http.Client{},
		waiters:    newWaiterTable(),
	}
}

type runReq struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
}

type runResp struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (b *Broker) HandleRun(w http.ResponseWriter, r *http.Request) {
	var req runReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, runResp{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.SessionID == "" || req.Input == "" {
		writeJSON(w, http.StatusBadRequest, runResp{Error: "sessionId and input are required"})
		return
	}

	key := dedupKey(req.SessionID, req.Input)
	log.Printf("broker: session=%s key=%s starting", req.SessionID, key)

	// Register a wake channel BEFORE the first upstream call so a /notify
	// that races with our suspend can't be lost.
	wake := b.waiters.register(key)
	defer b.waiters.unregister(key, wake)

	bodyBytes, _ := json.Marshal(req)

	for attempt := 1; ; attempt++ {
		resp, status, err := b.callAgent(r.Context(), req.SessionID, bodyBytes)
		if err == nil && status == http.StatusOK {
			log.Printf("broker: session=%s attempt=%d ok", req.SessionID, attempt)
			writeJSON(w, http.StatusOK, *resp)
			return
		}
		if err != nil && !isTransientUpstreamError(err) {
			log.Printf("broker: session=%s attempt=%d hard error: %v", req.SessionID, attempt, err)
			writeJSON(w, http.StatusBadGateway, runResp{Error: err.Error()})
			return
		}
		if err == nil && !isTransientUpstreamStatus(status) {
			log.Printf("broker: session=%s attempt=%d upstream status=%d", req.SessionID, attempt, status)
			writeJSON(w, status, *resp)
			return
		}

		log.Printf("broker: session=%s attempt=%d upstream unavailable (err=%v status=%d), waiting for /notify", req.SessionID, attempt, err, status)
		if !waitForWake(r.Context(), wake) {
			log.Printf("broker: session=%s client cancelled while waiting", req.SessionID)
			return
		}
		log.Printf("broker: session=%s wake signal received, retrying", req.SessionID)
	}
}

// HandleNotify is called by the agent after it caches a result, telling the
// broker to wake any parked client request on the matching key.
func (b *Broker) HandleNotify(w http.ResponseWriter, r *http.Request) {
	var req runReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.SessionID == "" || req.Input == "" {
		http.Error(w, "sessionId and input are required", http.StatusBadRequest)
		return
	}
	key := dedupKey(req.SessionID, req.Input)
	n := b.waiters.wake(key)
	log.Printf("broker: notify session=%s key=%s waiters=%d", req.SessionID, key, n)
	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) callAgent(ctx context.Context, sessionID string, body []byte) (*runResp, int, error) {
	url := fmt.Sprintf("http://%s/run", b.atenetAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = sessionID + actorHostSuffix

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read upstream body: %w", err)
	}
	var parsed runResp
	if len(raw) > 0 {
		if jerr := json.Unmarshal(raw, &parsed); jerr != nil {
			// Atenet on actor-unavailable typically replies with a non-JSON
			// 5xx body; preserve it for the client log.
			parsed.Error = string(raw)
		}
	}
	return &parsed, resp.StatusCode, nil
}

func waitForWake(ctx context.Context, wake <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		// Tiny pause so the agent has time to finish its response write to
		// the next /run before we hit it again on the retry.
		time.Sleep(50 * time.Millisecond)
		return true
	}
}

func isTransientUpstreamError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	// url.Error wraps lower-level errors; the matches above usually catch
	// them. Fall back to string match for the rest.
	s := err.Error()
	return containsAny(s, "connection refused", "connection reset", "EOF", "broken pipe", "no route to host")
}

func isTransientUpstreamStatus(status int) bool {
	switch status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && bytes.Contains([]byte(s), []byte(sub)) {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, body runResp) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func dedupKey(sessionID, input string) string {
	sum := sha256.Sum256([]byte(input))
	return sessionID + ":" + hex.EncodeToString(sum[:8])
}

// waiterTable maps dedup keys to per-request wake channels. Each /run call
// registers one channel; /notify closes every registered channel for a key
// at once.
type waiterTable struct {
	mu  sync.Mutex
	chs map[string]map[chan struct{}]struct{}
}

func newWaiterTable() *waiterTable {
	return &waiterTable{chs: make(map[string]map[chan struct{}]struct{})}
}

func (t *waiterTable) register(key string) chan struct{} {
	ch := make(chan struct{})
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.chs[key]
	if !ok {
		set = make(map[chan struct{}]struct{})
		t.chs[key] = set
	}
	set[ch] = struct{}{}
	return ch
}

func (t *waiterTable) unregister(key string, ch chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.chs[key]
	if !ok {
		return
	}
	delete(set, ch)
	if len(set) == 0 {
		delete(t.chs, key)
	}
}

// wake closes every channel currently registered for key. Returns the count
// woken (zero means no /run was waiting — fine; the original response is
// taking the result back the normal way).
func (t *waiterTable) wake(key string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.chs[key]
	if !ok {
		return 0
	}
	for ch := range set {
		safeClose(ch)
	}
	// Drop the set so a late /notify for the same key after all waiters
	// have unregistered does nothing.
	delete(t.chs, key)
	return len(set)
}

// safeClose closes ch unless it's already closed. The waiter goroutine never
// closes its own channel, so a double-close happens only if wake races with
// unregister — defensive guard for that.
func safeClose(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}
