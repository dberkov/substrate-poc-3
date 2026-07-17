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

// Package ateapi wraps substrate's Control gRPC client with the two calls
// this PoC needs — SuspendActor (from the sidecar) and ResumeActor (from
// the broker) — behind a small interface, so the rest of the code does not
// depend on the substrate proto directly and can be faked in tests.
package ateapi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Ref identifies an actor: its atespace and name. Substrate scopes actors
// by (atespace, name).
type Ref struct {
	Atespace string
	Name     string
}

func (r Ref) String() string { return r.Atespace + "/" + r.Name }

// Lifecycle is the suspend/resume surface used by the broker and sidecar.
// It is intentionally minimal so it can be faked in tests; the concrete
// Client additionally offers CreateActor/DeleteActor for the demo driver.
type Lifecycle interface {
	SuspendActor(ctx context.Context, actor Ref) error
	ResumeActor(ctx context.Context, actor Ref) error
}

// Client is a Lifecycle backed by substrate's ateapi Control service.
type Client struct {
	conn *grpc.ClientConn
	ctrl ateapipb.ControlClient
}

// Config configures Dial.
type Config struct {
	// Addr is the ateapi gRPC address (e.g. "api.ate-system.svc:443").
	Addr string
	// Insecure uses a plaintext connection instead of TLS. Substrate's
	// ateapi terminates TLS with a self-signed cert, so TLS mode uses
	// InsecureSkipVerify (matching the substrate demos).
	Insecure bool
}

// Dial connects to ateapi. Call Close when done.
func Dial(cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		return nil, errors.New("ateapi: Addr is required")
	}
	var creds credentials.TransportCredentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	}
	conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("ateapi: dial %s: %w", cfg.Addr, err)
	}
	return &Client{conn: conn, ctrl: ateapipb.NewControlClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// SuspendActor checkpoints the actor. Note: when called from inside the
// actor itself (the sidecar case), this RPC never returns cleanly — the
// caller freezes mid-call and the connection is a zombie on resume. Callers
// there should treat any error as expected.
func (c *Client) SuspendActor(ctx context.Context, actor Ref) error {
	_, err := c.ctrl.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: actor.Atespace, Name: actor.Name},
	})
	return err
}

// ResumeActor brings the actor back to RUNNING. Idempotent: substrate
// fast-paths when the actor is already running.
func (c *Client) ResumeActor(ctx context.Context, actor Ref) error {
	_, err := c.ctrl.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: actor.Atespace, Name: actor.Name},
	})
	return err
}

// CreateActor creates an actor from the given ActorTemplate
// (templateNamespace/templateName). Used by the demo driver, not the tunnel.
func (c *Client) CreateActor(ctx context.Context, actor Ref, templateNamespace, templateName string) error {
	_, err := c.ctrl.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplateNamespace: templateNamespace,
			ActorTemplateName:      templateName,
		},
	})
	return err
}

// DeleteActor deletes an actor. It must be suspended first (substrate
// requirement).
func (c *Client) DeleteActor(ctx context.Context, actor Ref) error {
	_, err := c.ctrl.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: actor.Atespace, Name: actor.Name},
	})
	return err
}
