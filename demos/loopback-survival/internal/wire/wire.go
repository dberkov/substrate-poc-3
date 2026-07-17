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

// Package wire is the tiny framed protocol spoken between the
// loopback-survival client and server over a single loopback TCP connection
// inside one actor. Every byte is accounted for: frames carry a strictly
// increasing sequence number and a per-payload CRC, and acks carry a running
// CRC over all payloads the server has accepted, so a suspend/resume cycle
// that drops, duplicates, or corrupts even a single byte is detected on
// either end.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// MaxPayload bounds a single frame payload so a corrupted length prefix
// cannot trigger a huge allocation.
const MaxPayload = 1 << 20

// ErrCorrupt is returned by ReadFrame when the payload does not match its
// CRC — i.e. bytes were altered in transit.
var ErrCorrupt = errors.New("wire: payload CRC mismatch")

// WriteFrame writes one data frame:
//
//	| u32 len(payload) | u64 seq | payload | u32 crc32(payload) |
func WriteFrame(w io.Writer, seq uint64, payload []byte) error {
	var hdr [12]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(hdr[4:12], seq)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	var tail [4]byte
	binary.BigEndian.PutUint32(tail[:], crc32.ChecksumIEEE(payload))
	_, err := w.Write(tail[:])
	return err
}

// ReadFrame reads one data frame written by WriteFrame and verifies the
// payload CRC.
func ReadFrame(r io.Reader) (seq uint64, payload []byte, err error) {
	var hdr [12]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	if n > MaxPayload {
		return 0, nil, fmt.Errorf("wire: frame payload %d exceeds max %d", n, MaxPayload)
	}
	seq = binary.BigEndian.Uint64(hdr[4:12])
	payload = make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	var tail [4]byte
	if _, err := io.ReadFull(r, tail[:]); err != nil {
		return 0, nil, err
	}
	if binary.BigEndian.Uint32(tail[:]) != crc32.ChecksumIEEE(payload) {
		return seq, payload, ErrCorrupt
	}
	return seq, payload, nil
}

// WriteAck writes one ack frame:
//
//	| u64 seq | u64 totalBytes | u32 runningCRC |
//
// where totalBytes and runningCRC cover every payload the server has
// accepted up to and including seq.
func WriteAck(w io.Writer, seq, totalBytes uint64, runningCRC uint32) error {
	var buf [20]byte
	binary.BigEndian.PutUint64(buf[0:8], seq)
	binary.BigEndian.PutUint64(buf[8:16], totalBytes)
	binary.BigEndian.PutUint32(buf[16:20], runningCRC)
	_, err := w.Write(buf[:])
	return err
}

// ReadAck reads one ack frame written by WriteAck.
func ReadAck(r io.Reader) (seq, totalBytes uint64, runningCRC uint32, err error) {
	var buf [20]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, 0, 0, err
	}
	return binary.BigEndian.Uint64(buf[0:8]),
		binary.BigEndian.Uint64(buf[8:16]),
		binary.BigEndian.Uint32(buf[16:20]),
		nil
}

// UpdateCRC extends a running CRC32 with payload. Both sides compute this
// independently; a divergence after resume means bytes were lost or altered.
func UpdateCRC(crc uint32, payload []byte) uint32 {
	return crc32.Update(crc, crc32.IEEETable, payload)
}

// FillPayload deterministically fills p from seq, so payload content is
// reproducible from the sequence number alone (a cheap xorshift; the point
// is only that corrupt bytes fail the CRC, not cryptographic quality).
func FillPayload(p []byte, seq uint64) {
	x := seq*0x9E3779B97F4A7C15 + 1
	for i := range p {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		p[i] = byte(x)
	}
}
