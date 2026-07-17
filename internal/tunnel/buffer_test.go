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

package tunnel

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

func TestReplayBufferBasics(t *testing.T) {
	b := NewReplayBuffer(10)
	if err := b.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if b.End() != 5 || b.Base() != 0 || b.Len() != 5 || b.Free() != 5 {
		t.Fatalf("unexpected state: base=%d end=%d len=%d free=%d", b.Base(), b.End(), b.Len(), b.Free())
	}

	got, err := b.From(2)
	if err != nil || string(got) != "llo" {
		t.Fatalf("From(2) = %q, %v", got, err)
	}

	b.TrimTo(3)
	if b.Base() != 3 || b.Len() != 2 {
		t.Fatalf("after TrimTo(3): base=%d len=%d", b.Base(), b.Len())
	}
	if _, err := b.From(2); err == nil {
		t.Fatal("From below base should error")
	}
	got, err = b.From(3)
	if err != nil || string(got) != "lo" {
		t.Fatalf("From(3) = %q, %v", got, err)
	}

	// Offsets keep accumulating across trims.
	if err := b.Append([]byte("worldxxx")); err != nil {
		t.Fatal(err)
	}
	if b.End() != 13 {
		t.Fatalf("End = %d, want 13", b.End())
	}
	if err := b.Append([]byte("y")); !errors.Is(err, ErrFull) {
		t.Fatalf("Append over cap = %v, want ErrFull", err)
	}
	b.TrimTo(13)
	if b.Len() != 0 || b.Base() != 13 || b.End() != 13 {
		t.Fatalf("after full trim: base=%d end=%d len=%d", b.Base(), b.End(), b.Len())
	}
}

func TestFrameRoundTrip(t *testing.T) {
	frames := []Frame{
		{Type: TypeHello, ActorID: "actor-1"},
		{Type: TypeOpen, SessionID: 7, Target: "mcp-server.svc:80"},
		{Type: TypeAttach, SessionID: 7, Offset: 4096, Target: "mcp-server.svc:80"},
		{Type: TypeAttachOK, SessionID: 7, Offset: 512},
		{Type: TypeData, SessionID: 7, Offset: 512, Payload: []byte("payload bytes")},
		{Type: TypeAck, SessionID: 7, Offset: 525},
		{Type: TypeClose, SessionID: 7, Dir: DirDown, Reason: "upstream EOF"},
		{Type: TypePing, Nonce: 42},
		{Type: TypePong, Nonce: 42},
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	for _, f := range frames {
		if err := encodeFrame(w, f); err != nil {
			t.Fatalf("encode %v: %v", f, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := bufio.NewReader(&buf)
	for _, want := range frames {
		got, err := decodeFrame(r)
		if err != nil {
			t.Fatalf("decode (want %v): %v", want, err)
		}
		if got.Type != want.Type || got.ActorID != want.ActorID ||
			got.SessionID != want.SessionID || got.Target != want.Target ||
			got.Offset != want.Offset || got.Dir != want.Dir ||
			got.Reason != want.Reason || got.Nonce != want.Nonce ||
			!bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("round trip mismatch:\n got %v\nwant %v", got, want)
		}
	}
}
