package webhookhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// DefaultMaxBodyBytes caps the raw request body the server is willing
// to buffer. Webhook payloads from real providers fit well under 64KB;
// rejecting anything larger defends against memory-exhaustion DoS.
const DefaultMaxBodyBytes int64 = 65_536

// Config bundles construction-time knobs. Zero-value Config produces
// a usable server when callers pass an Addr via WithAddr or directly
// set the field.
type Config struct {
	// Addr is the listen address (e.g. ":8082" or "127.0.0.1:8082").
	// Required.
	Addr string

	// TLSConfig, when non-nil, switches the server to TLS. The
	// caller is responsible for populating Certificates / ClientCAs
	// (for mTLS use cases).
	TLSConfig *tls.Config

	// MaxBodyBytes caps the per-request body the server will buffer.
	// Zero ⇒ DefaultMaxBodyBytes. Set higher only with intent.
	MaxBodyBytes int64

	// ReadTimeout / WriteTimeout / IdleTimeout match net/http.Server
	// fields. Zero falls back to library defaults below.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// Delivery is the per-request payload a Handler sees. Body has
// already been buffered (and size-checked) by the server.
type Delivery struct {
	Body           []byte
	Headers        http.Header
	IdempotencyKey string
}

// Handler processes one webhook delivery.
//
// Return values map to HTTP status codes:
//
//	nil                         → 202 Accepted
//	ErrDuplicate                → 200 OK (and an empty body)
//	*TerminalError              → 400 Bad Request
//	any other error             → 500 Internal Server Error
type Handler func(ctx context.Context, d Delivery) error

// ErrDuplicate signals the delivery is a known duplicate and the
// caller wants the server to respond 200 (so the provider stops
// retrying) without further processing.
var ErrDuplicate = errors.New("webhookhttp: duplicate delivery")

// TerminalError signals a permanent failure (bad signature, malformed
// payload, schema mismatch). The server emits HTTP 400.
type TerminalError struct{ Cause error }

func (e *TerminalError) Error() string {
	if e == nil || e.Cause == nil {
		return "webhookhttp: terminal error"
	}
	return e.Cause.Error()
}

func (e *TerminalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// HandlerOption customizes a single Handle registration.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	verify        func(r *http.Request, body []byte) error
	idempotencyFn func(r *http.Request, body []byte) string
	maxBodyBytes  int64
}

// WithVerifyFunc sets a signature/auth verifier called BEFORE the
// handler runs. Returning a non-nil error short-circuits with HTTP
// 401. The function receives the buffered body so HMAC checks can
// hash the exact bytes the provider signed.
func WithVerifyFunc(fn func(r *http.Request, body []byte) error) HandlerOption {
	return func(c *handlerConfig) { c.verify = fn }
}

// WithIdempotencyKey extracts an idempotency key (from headers or
// body) and stores it on Delivery so the handler can dedup. Returning
// "" disables per-request dedup signal.
func WithIdempotencyKey(fn func(r *http.Request, body []byte) string) HandlerOption {
	return func(c *handlerConfig) { c.idempotencyFn = fn }
}

// WithMaxBodyBytes overrides the per-route body limit. Useful when
// one route accepts larger payloads than the global Config default.
func WithMaxBodyBytes(n int64) HandlerOption {
	return func(c *handlerConfig) { c.maxBodyBytes = n }
}

// Server wraps net/http.Server with the webhook-oriented framing
// described in the package doc.
type Server struct {
	cfg Config

	mu     sync.Mutex
	routes map[string]map[string]*route // method → path → route
}

type route struct {
	handler Handler
	opts    handlerConfig
}

// New constructs a Server. Addr is the only required field on cfg.
// Side-effects (binding the listen socket) are deferred to
// ListenAndServe so construction stays test-friendly.
func New(cfg Config) *Server {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 30 * time.Second
	}
	return &Server{
		cfg:    cfg,
		routes: map[string]map[string]*route{},
	}
}

// Handle registers (method, path) → handler. Repeated calls overwrite,
// matching net/http.ServeMux semantics. Returns the same Server for
// chaining.
func (s *Server) Handle(method, path string, h Handler, opts ...HandlerOption) *Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	hc := handlerConfig{maxBodyBytes: s.cfg.MaxBodyBytes}
	for _, opt := range opts {
		opt(&hc)
	}
	if hc.maxBodyBytes <= 0 {
		hc.maxBodyBytes = s.cfg.MaxBodyBytes
	}
	if s.routes[method] == nil {
		s.routes[method] = map[string]*route{}
	}
	s.routes[method][path] = &route{handler: h, opts: hc}
	return s
}

// ListenAndServe binds the listen socket and serves until ctx is
// cancelled. Returns nil on graceful shutdown; otherwise the net/http
// error.
func (s *Server) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()
	s.mu.Lock()
	// Group routes by path; the per-path handler dispatches by method.
	paths := map[string]map[string]*route{}
	for method, byPath := range s.routes {
		for path, r := range byPath {
			if paths[path] == nil {
				paths[path] = map[string]*route{}
			}
			paths[path][method] = r
		}
	}
	s.mu.Unlock()

	for path, byMethod := range paths {
		path := path
		byMethod := byMethod
		mux.HandleFunc(path, func(w http.ResponseWriter, req *http.Request) {
			r, ok := byMethod[req.Method]
			if !ok {
				// Build Allow header from registered methods.
				allow := ""
				for m := range byMethod {
					if allow != "" {
						allow += ", "
					}
					allow += m
				}
				w.Header().Set("Allow", allow)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.serveRoute(w, req, r)
		})
	}

	httpSrv := &http.Server{
		Addr:         s.cfg.Addr,
		Handler:      mux,
		TLSConfig:    s.cfg.TLSConfig,
		ReadTimeout:  s.cfg.ReadTimeout,
		WriteTimeout: s.cfg.WriteTimeout,
		IdleTimeout:  s.cfg.IdleTimeout,
	}

	serveErr := make(chan error, 1)
	go func() {
		if s.cfg.TLSConfig != nil {
			serveErr <- httpSrv.ListenAndServeTLS("", "")
		} else {
			serveErr <- httpSrv.ListenAndServe()
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		// Drain the serve goroutine.
		select {
		case <-serveErr:
		case <-time.After(6 * time.Second):
		}
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("webhookhttp: serve: %w", err)
	}
}

func (s *Server) serveRoute(w http.ResponseWriter, req *http.Request, r *route) {
	limit := r.opts.maxBodyBytes
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, limit))
	if err != nil {
		http.Error(w, "body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	if r.opts.verify != nil {
		if err := r.opts.verify(req, body); err != nil {
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}
	}

	d := Delivery{Body: body, Headers: req.Header}
	if r.opts.idempotencyFn != nil {
		d.IdempotencyKey = r.opts.idempotencyFn(req, body)
	}

	switch err := r.handler(req.Context(), d); {
	case err == nil:
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, ErrDuplicate):
		w.WriteHeader(http.StatusOK)
	default:
		var te *TerminalError
		if errors.As(err, &te) {
			http.Error(w, te.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "internal", http.StatusInternalServerError)
	}
}
