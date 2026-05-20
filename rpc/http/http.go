// Package http implements rpc.Transport over HTTP. This is the
// broker-free transport: request/response over a single HTTP mux
// where each Consume call registers a POST handler at
// /rpc/<endpoint>. Request dispatches an HTTP POST and reads the
// body as the Reply.
//
// Use cases:
//
//   - A deployment that prefers not to run a broker.
//   - An adapter exposed as an HTTP service (Kubernetes Service,
//     cloud Function, sidecar). Its integration_type declares
//     `adapter.transport: http_json` and Endpoints.
//   - Local / in-process testing: Server and Client can share a
//     mux, so the entire RPC round-trip runs synchronously without
//     a network hop.
//
// Envelope framing: HTTP has no correlation-id / reply-to fields
// native to the protocol, so the transport serializes an
// rpc.Envelope as the POST body and expects the same shape in the
// response. Handlers receive an rpc.Delivery with CorrelationID
// and Headers unpacked from the envelope; Reply encodes another
// envelope into the HTTP response writer.
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

// PathPrefix is the URL prefix every RPC endpoint registers under.
// Set once at transport construction and then immutable — clients
// and servers agree on it via their shared config.
const DefaultPathPrefix = "/rpc/"

// Transport is the HTTP-backed rpc.Transport.
//
// The Server side registers handlers on a mux (typically the core's
// own HTTP mux, so RPC shares the same listener as the public API).
// The Client side POSTs envelopes; for in-process calls the client
// can short-circuit through the mux without touching the network.
type Transport struct {
	// BaseURL is the origin clients dial for outbound Request /
	// Publish. Must be non-empty for Request; can be empty for
	// Server-only or in-process mode (where a mux is the loopback).
	BaseURL string

	// Mux is where Consume registers handlers. Supply your shared
	// http.ServeMux (e.g., the core's). Required if Consume is
	// called; optional for client-only usage.
	Mux *http.ServeMux

	// Client is the http.Client used for outbound Request /
	// Publish. Defaults to a Client with a sensible 30s timeout
	// when nil.
	Client *http.Client

	// PathPrefix overrides DefaultPathPrefix. Rare; useful for
	// version-prefixing ("/rpc/v1/") without leaking into
	// endpoint names.
	PathPrefix string

	subsMu sync.Mutex
	subs   []*subscription

	closed   bool
	closeMu  sync.Mutex
}

// New builds a Transport. Either Mux (for serving), BaseURL (for
// client), or both must be set.
//
// Accepts a *Transport (not a value) so the call site can never
// accidentally copy the embedded sync.Mutexes (subsMu, closeMu). The
// function returns a freshly-allocated Transport — the input is read
// only for its configuration fields. Pass a composite-literal pointer:
//
//	t := sdkhttp.New(&sdkhttp.Transport{Mux: mux})
func New(opts *Transport) *Transport {
	if opts == nil {
		opts = &Transport{}
	}
	t := &Transport{
		Mux:        opts.Mux,
		BaseURL:    opts.BaseURL,
		Client:     opts.Client,
		PathPrefix: opts.PathPrefix,
	}
	if t.PathPrefix == "" {
		t.PathPrefix = DefaultPathPrefix
	}
	if t.Client == nil {
		t.Client = &http.Client{Timeout: 30 * time.Second}
	}
	return t
}

// Consume registers the handler on the mux. The endpoint becomes
// the URL path suffix; incoming POSTs are decoded as rpc.Envelope,
// dispatched to the handler, and the response envelope is written
// back on the same HTTP response.
func (t *Transport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("http consume: endpoint is required")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("http consume: handler is required")
	}
	if t.Mux == nil {
		return nil, fmt.Errorf("http consume: Mux is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}

	sub := &subscription{
		transport: t,
		endpoint:  cfg.Endpoint,
		sem:       make(chan struct{}, cfg.Concurrency),
		timeout:   cfg.Timeout,
		handler:   cfg.Handler,
	}

	path := t.PathPrefix + cfg.Endpoint
	t.Mux.HandleFunc("POST "+path, sub.serveHTTP)

	t.subsMu.Lock()
	t.subs = append(t.subs, sub)
	t.subsMu.Unlock()

	return sub, nil
}

// Request sends the request envelope to BaseURL + PathPrefix +
// Endpoint and returns the decoded reply. Timeout on req overrides
// the transport-default.
func (t *Transport) Request(ctx context.Context, req rpc.Request) (rpc.Reply, error) {
	if req.Endpoint == "" {
		return rpc.Reply{}, fmt.Errorf("http request: endpoint is required")
	}
	if t.BaseURL == "" {
		return rpc.Reply{}, fmt.Errorf("http request: BaseURL is required")
	}

	envelope := rpc.Envelope{
		CorrelationID: req.CorrelationID,
		Headers:       req.Headers,
		ContentType:   req.ContentType,
		Body:          req.Body,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("http request: encode envelope: %w", err)
	}

	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	url := strings.TrimRight(t.BaseURL, "/") + t.PathPrefix + req.Endpoint
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("http request: build: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := t.Client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return rpc.Reply{}, rpc.ErrTimeout
		}
		return rpc.Reply{}, fmt.Errorf("http request: do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("http request: read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return rpc.Reply{}, rpc.ErrEndpointUnknown
	}
	if resp.StatusCode >= 400 {
		return rpc.Reply{}, fmt.Errorf("http request: remote returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var replyEnvelope rpc.Envelope
	if err := json.Unmarshal(respBody, &replyEnvelope); err != nil {
		// Some adapters reply raw (no envelope). Return the raw
		// body as the Reply body and preserve correlation on the
		// way out.
		return rpc.Reply{
			Body:          respBody,
			ContentType:   resp.Header.Get("Content-Type"),
			CorrelationID: req.CorrelationID,
		}, nil
	}

	return rpc.Reply{
		Body:          replyEnvelope.Body,
		ContentType:   replyEnvelope.ContentType,
		Headers:       replyEnvelope.Headers,
		CorrelationID: replyEnvelope.CorrelationID,
	}, nil
}

// Publish sends a fire-and-forget envelope. No reply is awaited —
// the transport issues the POST and returns once the server
// acknowledges with a 2xx status.
func (t *Transport) Publish(ctx context.Context, req rpc.Request) error {
	// Publish shares the Request round-trip but ignores the reply
	// body. Keeping it symmetric avoids one-off branches in the
	// http package and keeps error semantics identical for callers.
	_, err := t.Request(ctx, req)
	if errors.Is(err, rpc.ErrEndpointUnknown) {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

// Close unregisters every subscription and releases the HTTP
// client's idle connections. Safe to call multiple times.
func (t *Transport) Close() error {
	t.closeMu.Lock()
	if t.closed {
		t.closeMu.Unlock()
		return nil
	}
	t.closed = true
	t.closeMu.Unlock()

	t.subsMu.Lock()
	subs := append([]*subscription(nil), t.subs...)
	t.subs = nil
	t.subsMu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}

	if t.Client != nil && t.Client.Transport != nil {
		if closer, ok := t.Client.Transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	}
	return nil
}

// subscription is one registered POST handler.
type subscription struct {
	transport *Transport
	endpoint  string
	sem       chan struct{}
	timeout   time.Duration
	handler   rpc.Handler

	closedMu sync.RWMutex
	closed   bool
}

func (s *subscription) Endpoint() string { return s.endpoint }

func (s *subscription) Close() error {
	s.closedMu.Lock()
	s.closed = true
	s.closedMu.Unlock()
	// http.ServeMux cannot unregister handlers; the handler checks
	// s.closed on each invocation and returns 503 when stopped.
	return nil
}

// serveHTTP decodes the inbound envelope, invokes the handler, and
// serializes the response envelope back to the HTTP response.
func (s *subscription) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.closedMu.RLock()
	if s.closed {
		s.closedMu.RUnlock()
		http.Error(w, "subscription closed", http.StatusServiceUnavailable)
		return
	}
	s.closedMu.RUnlock()

	// Throttle to the configured concurrency. HTTP transports
	// typically have LB-level concurrency controls too; this is
	// belt-and-suspenders for the adapter pod's own capacity.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-r.Context().Done():
		http.Error(w, "request canceled", http.StatusServiceUnavailable)
		return
	}

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}

	var envelope rpc.Envelope
	if len(raw) > 0 {
		// Try to decode as Envelope first. An Envelope has no
		// required fields, so a random JSON object decodes
		// "successfully" with every field empty. Detect that and
		// fall back to treating the entire body as envelope.Body
		// — the simple-client path (cURL, legacy adapter, external
		// integration that doesn't know about Envelope framing).
		if err := json.Unmarshal(raw, &envelope); err != nil ||
			(len(envelope.Body) == 0 && envelope.CorrelationID == "" &&
				len(envelope.Headers) == 0 && envelope.ReplyTo == "") {
			envelope = rpc.Envelope{Body: raw}
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	// Build the Delivery. Reply wraps the ResponseWriter — writing
	// the envelope once on the first call, ignoring subsequent
	// calls so handlers can defensively reply-then-ack.
	var (
		replied     bool
		replyHeader = w.Header()
	)
	replyEnvelope := rpc.Envelope{CorrelationID: envelope.CorrelationID}

	reply := func(ctx context.Context, body []byte, contentType string) error {
		if replied {
			return nil
		}
		replied = true
		replyEnvelope.Body = body
		replyEnvelope.ContentType = contentType
		out, err := json.Marshal(replyEnvelope)
		if err != nil {
			return fmt.Errorf("encode reply envelope: %w", err)
		}
		replyHeader.Set("Content-Type", "application/json")
		_, writeErr := w.Write(out)
		return writeErr
	}

	ack := func() error { return nil }
	// Nack with requeue=false: return a 500 so the client sees the
	// failure; with requeue=true: return 503 signaling "try again".
	// Because HTTP has no native queueing, we cannot really requeue
	// — this is the best-effort semantics.
	nack := func(requeue bool) error {
		if replied {
			return nil
		}
		replied = true
		status := http.StatusInternalServerError
		if requeue {
			status = http.StatusServiceUnavailable
		}
		w.WriteHeader(status)
		return nil
	}

	d := rpc.Delivery{
		Endpoint:      s.endpoint,
		Body:          envelope.Body,
		ContentType:   envelope.ContentType,
		Headers:       envelope.Headers,
		CorrelationID: envelope.CorrelationID,
		ReplyTo:       "http-response", // synthetic; HTTP uses the response writer
		AckFn:         ack,
		NackFn:        nack,
		ReplyFn:       reply,
	}

	if err := s.handler(ctx, d); err != nil {
		if !replied {
			replied = true
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("handler error: %v", err)))
		}
		return
	}

	// Handler returned successfully but never called Reply. Some
	// handlers are fire-and-forget (Publish on the client side);
	// send an empty envelope so the client's 200 is honored.
	if !replied {
		empty, _ := json.Marshal(rpc.Envelope{CorrelationID: envelope.CorrelationID})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(empty)
	}
}
