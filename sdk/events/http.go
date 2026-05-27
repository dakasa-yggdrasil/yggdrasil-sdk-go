package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// EnvCoreURL is the environment variable NewHTTPEmitter consults for
// the yggdrasil-core base URL when WithCoreURL is not provided.
const EnvCoreURL = "YGGDRASIL_CORE_URL"

// EnvRunToken is the environment variable NewHTTPEmitter consults
// for the bearer token when WithToken is not provided. The same
// token adapter pods use to authenticate workflow runs against
// yggdrasil-core.
const EnvRunToken = "YGGDRASIL_RUN_TOKEN"

// DefaultEventsPath is the yggdrasil-core HTTP endpoint POST'ed to.
const DefaultEventsPath = "/api/v1/events"

// DefaultMaxRetries bounds the total number of HTTP attempts per
// Emit call. The first attempt counts; 3 means 1 + 2 retries.
const DefaultMaxRetries = 3

// DefaultRetryBackoff is the base backoff between retry attempts.
// The HTTP emitter uses a flat (not exponential) backoff so emission
// stays cheap and predictable; callers tune via WithRetryBackoff.
const DefaultRetryBackoff = 250 * time.Millisecond

// DefaultHTTPTimeout bounds a single HTTP request the emitter issues.
const DefaultHTTPTimeout = 5 * time.Second

// Option mutates an httpEmitter during construction.
type Option func(*httpEmitter)

// WithCoreURL overrides the yggdrasil-core base URL. When unset, the
// emitter falls back to the YGGDRASIL_CORE_URL env var.
func WithCoreURL(url string) Option {
	return func(e *httpEmitter) { e.coreURL = url }
}

// WithToken overrides the bearer token. When unset, the emitter
// falls back to the YGGDRASIL_RUN_TOKEN env var.
func WithToken(token string) Option {
	return func(e *httpEmitter) { e.token = token }
}

// WithHTTPClient swaps the underlying http.Client. Useful for tests
// (round-tripper injection) and for adapters that want to share a
// pooled client with their own outbound calls.
func WithHTTPClient(c *http.Client) Option {
	return func(e *httpEmitter) { e.client = c }
}

// WithMaxRetries overrides the total attempt budget. Values <= 0 fall
// back to DefaultMaxRetries.
func WithMaxRetries(n int) Option {
	return func(e *httpEmitter) { e.maxRetries = n }
}

// WithRetryBackoff overrides the fixed delay between retry attempts.
// Values <= 0 fall back to DefaultRetryBackoff.
func WithRetryBackoff(d time.Duration) Option {
	return func(e *httpEmitter) { e.retryBackoff = d }
}

// WithEventsPath overrides the path appended to the base URL when
// posting events. Defaults to "/api/v1/events".
func WithEventsPath(path string) Option {
	return func(e *httpEmitter) { e.eventsPath = path }
}

// httpEmitter implements Emitter against yggdrasil-core's
// POST /api/v1/events endpoint.
type httpEmitter struct {
	coreURL      string
	eventsPath   string
	token        string
	client       *http.Client
	maxRetries   int
	retryBackoff time.Duration
}

// NewHTTPEmitter constructs an emitter targeting yggdrasil-core. The
// emitter reads YGGDRASIL_CORE_URL + YGGDRASIL_RUN_TOKEN when those
// fields aren't overridden by options. POSTs to /api/v1/events; retries
// transient 5xx; treats 4xx as terminal; honors context cancellation.
func NewHTTPEmitter(opts ...Option) Emitter {
	e := &httpEmitter{
		coreURL:      os.Getenv(EnvCoreURL),
		token:        os.Getenv(EnvRunToken),
		eventsPath:   DefaultEventsPath,
		maxRetries:   DefaultMaxRetries,
		retryBackoff: DefaultRetryBackoff,
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.client == nil {
		e.client = &http.Client{Timeout: DefaultHTTPTimeout}
	}
	if e.maxRetries <= 0 {
		e.maxRetries = DefaultMaxRetries
	}
	if e.retryBackoff <= 0 {
		e.retryBackoff = DefaultRetryBackoff
	}
	if e.eventsPath == "" {
		e.eventsPath = DefaultEventsPath
	}
	return e
}

// Emit posts e to yggdrasil-core. Returns nil on 2xx; an error
// describing the terminal failure otherwise.
func (h *httpEmitter) Emit(ctx context.Context, e MutationEvent) error {
	if e.EmittedAt.IsZero() {
		e.EmittedAt = time.Now().UTC()
	}
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("events: marshal mutation event: %w", err)
	}

	url := strings.TrimRight(h.coreURL, "/") + h.eventsPath

	var lastErr error
	for attempt := 0; attempt < h.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("events: context cancelled during retry backoff: %w", ctx.Err())
			case <-time.After(h.retryBackoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("events: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if h.token != "" {
			req.Header.Set("Authorization", "Bearer "+h.token)
		}

		resp, err := h.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("events: POST %s: %w", url, err)
			if ctx.Err() != nil {
				return lastErr
			}
			continue
		}
		// Drain and close to allow connection reuse.
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return nil
		case resp.StatusCode >= 400 && resp.StatusCode < 500:
			// Terminal: 4xx says the payload or auth is wrong;
			// retrying won't help.
			return fmt.Errorf("events: POST %s: terminal status %d: %s",
				url, resp.StatusCode, truncate(string(respBody), 256))
		default:
			// 5xx or 3xx: retry until budget exhausted.
			lastErr = fmt.Errorf("events: POST %s: transient status %d: %s",
				url, resp.StatusCode, truncate(string(respBody), 256))
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("events: POST %s: exhausted retries with no error captured", url)
	}
	return lastErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
