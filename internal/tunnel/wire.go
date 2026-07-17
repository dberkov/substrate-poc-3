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

// Package tunnel is the resumable byte-stream protocol spoken between the
// egress-sidecar (inside an actor) and the egress-broker (outside), per
// DESIGN.md §5.
//
// One tunnel TCP connection carries all of an actor's sessions. A session
// is one agent-side TCP connection: an opaque, offset-addressed byte stream
// in each direction. The tunnel connection dies on every actor suspend (the
// external network path is torn down silently); sessions do NOT die — both
// ends buffer un-acked bytes and splice the streams back together after the
// sidecar re-dials and ATTACHes, replaying from the offsets the peer
// declares. The agent-side connection, and the broker's upstream
// connection, never observe any of this.
package tunnel

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Type identifies a frame.
type Type uint8

const (
	// TypeHello (sidecar→broker) opens a tunnel connection: "I speak for
	// actor X". Sent exactly once, first, on every tunnel connection.
	TypeHello Type = iota + 1
	// TypeOpen (sidecar→broker) creates a session: the broker dials Target.
	TypeOpen
	// TypeAttach (sidecar→broker) re-adopts a session after a reconnect.
	// Offset = downstream bytes the sidecar has delivered to the agent.
	// Target is included so a session whose OPEN was lost to a zombie
	// connection can be recreated from scratch (only if Offset is 0).
	TypeAttach
	// TypeAttachOK (broker→sidecar) confirms an ATTACH. Offset = upstream
	// bytes the broker has written to the destination; the sidecar replays
	// everything after that.
	TypeAttachOK
	// TypeData carries payload bytes at Offset (cumulative per session per
	// direction).
	TypeData
	// TypeAck confirms cumulative receipt/processing up to Offset; the
	// sender may trim its replay buffer.
	TypeAck
	// TypeClose signals half-stream shutdown (Dir) with a Reason.
	TypeClose
	// TypePing / TypePong carry liveness. A checkpointed-then-restored
	// tunnel socket is a zombie that swallows writes silently — missed
	// PONGs are how the sidecar detects it.
	TypePing
	TypePong
)

// Close directions.
const (
	// DirUp: the agent→upstream half is closed (agent finished sending).
	DirUp uint8 = 1
	// DirDown: the upstream→agent half is closed (destination finished or
	// died).
	DirDown uint8 = 2
)

// MaxPayload bounds one DATA frame.
const MaxPayload = 256 * 1024

const maxString = 4096

// Frame is the union of all frame fields; Type says which are meaningful.
type Frame struct {
	Type      Type
	ActorID   string // Hello
	SessionID uint64 // all except Hello/Ping/Pong
	Target    string // Open, Attach
	Offset    uint64 // Data, Ack, Attach, AttachOK
	Payload   []byte // Data
	Dir       uint8  // Close
	Reason    string // Close
	Nonce     uint64 // Ping, Pong
}

func (f Frame) String() string {
	switch f.Type {
	case TypeHello:
		return fmt.Sprintf("HELLO(actor=%s)", f.ActorID)
	case TypeOpen:
		return fmt.Sprintf("OPEN(sid=%d target=%s)", f.SessionID, f.Target)
	case TypeAttach:
		return fmt.Sprintf("ATTACH(sid=%d delivered=%d target=%s)", f.SessionID, f.Offset, f.Target)
	case TypeAttachOK:
		return fmt.Sprintf("ATTACH_OK(sid=%d written=%d)", f.SessionID, f.Offset)
	case TypeData:
		return fmt.Sprintf("DATA(sid=%d off=%d len=%d)", f.SessionID, f.Offset, len(f.Payload))
	case TypeAck:
		return fmt.Sprintf("ACK(sid=%d off=%d)", f.SessionID, f.Offset)
	case TypeClose:
		return fmt.Sprintf("CLOSE(sid=%d dir=%d reason=%q)", f.SessionID, f.Dir, f.Reason)
	case TypePing:
		return fmt.Sprintf("PING(%d)", f.Nonce)
	case TypePong:
		return fmt.Sprintf("PONG(%d)", f.Nonce)
	}
	return fmt.Sprintf("UNKNOWN(%d)", f.Type)
}

// Conn wraps one tunnel TCP connection. Reads are single-goroutine (the
// owner's frame loop); writes are serialized by an internal mutex and
// carry a deadline so a zombie socket cannot wedge a writer forever.
type Conn struct {
	c  net.Conn
	br *bufio.Reader

	mu sync.Mutex
	bw *bufio.Writer

	// WriteTimeout bounds each frame write (default 10s).
	WriteTimeout time.Duration
}

func NewConn(c net.Conn) *Conn {
	return &Conn{
		c:            c,
		br:           bufio.NewReaderSize(c, 64*1024),
		bw:           bufio.NewWriterSize(c, 64*1024),
		WriteTimeout: 10 * time.Second,
	}
}

func (c *Conn) Close() error { return c.c.Close() }

func (c *Conn) RemoteAddr() net.Addr { return c.c.RemoteAddr() }

// SetReadDeadline delegates to the underlying connection (used by frame
// loops that want an idle bound).
func (c *Conn) SetReadDeadline(t time.Time) error { return c.c.SetReadDeadline(t) }

// WriteFrame encodes and flushes one frame.
func (c *Conn) WriteFrame(f Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.WriteTimeout > 0 {
		_ = c.c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	}
	if err := encodeFrame(c.bw, f); err != nil {
		return err
	}
	return c.bw.Flush()
}

// ReadFrame decodes the next frame. Not safe for concurrent use.
func (c *Conn) ReadFrame() (Frame, error) {
	return decodeFrame(c.br)
}

func encodeFrame(w *bufio.Writer, f Frame) error {
	if err := w.WriteByte(byte(f.Type)); err != nil {
		return err
	}
	switch f.Type {
	case TypeHello:
		return putString(w, f.ActorID)
	case TypeOpen:
		putU64(w, f.SessionID)
		return putString(w, f.Target)
	case TypeAttach:
		putU64(w, f.SessionID)
		putU64(w, f.Offset)
		return putString(w, f.Target)
	case TypeAttachOK, TypeData, TypeAck:
		putU64(w, f.SessionID)
		putU64(w, f.Offset)
		if f.Type == TypeData {
			if len(f.Payload) > MaxPayload {
				return fmt.Errorf("tunnel: payload %d exceeds max %d", len(f.Payload), MaxPayload)
			}
			putU32(w, uint32(len(f.Payload)))
			_, err := w.Write(f.Payload)
			return err
		}
		return nil
	case TypeClose:
		putU64(w, f.SessionID)
		if err := w.WriteByte(f.Dir); err != nil {
			return err
		}
		return putString(w, f.Reason)
	case TypePing, TypePong:
		putU64(w, f.Nonce)
		return nil
	}
	return fmt.Errorf("tunnel: cannot encode frame type %d", f.Type)
}

func decodeFrame(r *bufio.Reader) (Frame, error) {
	var f Frame
	t, err := r.ReadByte()
	if err != nil {
		return f, err
	}
	f.Type = Type(t)
	switch f.Type {
	case TypeHello:
		f.ActorID, err = getString(r)
		return f, err
	case TypeOpen:
		if f.SessionID, err = getU64(r); err != nil {
			return f, err
		}
		f.Target, err = getString(r)
		return f, err
	case TypeAttach:
		if f.SessionID, err = getU64(r); err != nil {
			return f, err
		}
		if f.Offset, err = getU64(r); err != nil {
			return f, err
		}
		f.Target, err = getString(r)
		return f, err
	case TypeAttachOK, TypeAck:
		if f.SessionID, err = getU64(r); err != nil {
			return f, err
		}
		f.Offset, err = getU64(r)
		return f, err
	case TypeData:
		if f.SessionID, err = getU64(r); err != nil {
			return f, err
		}
		if f.Offset, err = getU64(r); err != nil {
			return f, err
		}
		n, err := getU32(r)
		if err != nil {
			return f, err
		}
		if n > MaxPayload {
			return f, fmt.Errorf("tunnel: payload %d exceeds max %d", n, MaxPayload)
		}
		f.Payload = make([]byte, n)
		_, err = io.ReadFull(r, f.Payload)
		return f, err
	case TypeClose:
		if f.SessionID, err = getU64(r); err != nil {
			return f, err
		}
		if f.Dir, err = r.ReadByte(); err != nil {
			return f, err
		}
		f.Reason, err = getString(r)
		return f, err
	case TypePing, TypePong:
		f.Nonce, err = getU64(r)
		return f, err
	}
	return f, fmt.Errorf("tunnel: unknown frame type %d", f.Type)
}

func putU32(w *bufio.Writer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	_, _ = w.Write(b[:])
}

func putU64(w *bufio.Writer, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, _ = w.Write(b[:])
}

func putString(w *bufio.Writer, s string) error {
	if len(s) > maxString {
		return fmt.Errorf("tunnel: string field %d exceeds max %d", len(s), maxString)
	}
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(len(s)))
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	_, err := w.WriteString(s)
	return err
}

func getU32(r *bufio.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func getU64(r *bufio.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func getString(r *bufio.Reader) (string, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint16(b[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
