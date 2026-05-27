package webhookhttp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/webhookhttp"
)

// freeAddr returns a localhost address with a free TCP port. Avoids
// hard-coding :8082 (which would conflict with developer environments).
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestServer_RegisterAndServe_PostHandler(t *testing.T) {
	addr := freeAddr(t)
	var calls int32
	var seenBody []byte

	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			atomic.AddInt32(&calls, 1)
			seenBody = append(seenBody[:0], d.Body...)
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()

	// Wait until the server is accepting connections.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.Dial("tcp", addr); err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	resp, err := http.Post("http://"+addr+"/webhook", "application/json", strings.NewReader(`{"hi":1}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected handler called 1 time, got %d", calls)
	}
	if string(seenBody) != `{"hi":1}` {
		t.Fatalf("body mismatch: got %q", string(seenBody))
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("ListenAndServe returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down on context cancel within 2s")
	}
}

// Avoid an unused-import warning if io is not used yet.
var _ = io.Discard

func TestServer_VerifyFuncRejectsWith401(t *testing.T) {
	addr := freeAddr(t)
	verifyErr := errors.New("bad sig")

	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			t.Fatal("handler should not be reached when verify fails")
			return nil
		}, webhookhttp.WithVerifyFunc(func(r *http.Request, body []byte) error {
			return verifyErr
		}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	resp, err := http.Post("http://"+addr+"/webhook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestServer_MaxBodyBytesEnforced(t *testing.T) {
	addr := freeAddr(t)
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr, MaxBodyBytes: 16}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			t.Fatal("handler should not be reached when body exceeds limit")
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	big := strings.Repeat("X", 1024)
	resp, err := http.Post("http://"+addr+"/webhook", "application/octet-stream", strings.NewReader(big))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
}

func TestServer_MethodMismatchReturns405(t *testing.T) {
	addr := freeAddr(t)
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	resp, err := http.Get("http://" + addr + "/webhook")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Allow") != "POST" {
		t.Fatalf("expected Allow: POST, got %q", resp.Header.Get("Allow"))
	}
}

func TestServer_GetAndPostOnSamePath(t *testing.T) {
	// EFI sends a GET probe to /efi/webhook/pix and then a POST callback.
	// Both must succeed on the same path under different methods.
	addr := freeAddr(t)
	var posts int32
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("GET", "/efi/webhook/pix", func(ctx context.Context, d webhookhttp.Delivery) error {
			return nil
		}).
		Handle("POST", "/efi/webhook/pix", func(ctx context.Context, d webhookhttp.Delivery) error {
			atomic.AddInt32(&posts, 1)
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	get, _ := http.Get("http://" + addr + "/efi/webhook/pix")
	if get.StatusCode != http.StatusAccepted {
		t.Fatalf("GET expected 202, got %d", get.StatusCode)
	}
	get.Body.Close()

	post, _ := http.Post("http://"+addr+"/efi/webhook/pix", "application/json", strings.NewReader(`{}`))
	if post.StatusCode != http.StatusAccepted {
		t.Fatalf("POST expected 202, got %d", post.StatusCode)
	}
	post.Body.Close()

	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("expected POST handler called once, got %d", posts)
	}
}

// waitListen blocks until something accepts TCP on addr, up to 2s.
func waitListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.Dial("tcp", addr); err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server never accepted on %s", addr)
}

func TestServer_DuplicateReturns200(t *testing.T) {
	addr := freeAddr(t)
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			return webhookhttp.ErrDuplicate
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	resp, err := http.Post("http://"+addr+"/webhook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on ErrDuplicate, got %d", resp.StatusCode)
	}
}

func TestServer_TerminalErrorReturns400(t *testing.T) {
	addr := freeAddr(t)
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			return &webhookhttp.TerminalError{Cause: errors.New("schema invalid")}
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	resp, err := http.Post("http://"+addr+"/webhook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on TerminalError, got %d", resp.StatusCode)
	}
}

func TestServer_GenericErrorReturns500(t *testing.T) {
	addr := freeAddr(t)
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			return errors.New("kaboom")
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	waitListen(t, addr)

	resp, err := http.Post("http://"+addr+"/webhook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 on generic error, got %d", resp.StatusCode)
	}
}

func TestServer_GracefulShutdown_ContextCancellation(t *testing.T) {
	addr := freeAddr(t)
	srv := webhookhttp.New(webhookhttp.Config{Addr: addr}).
		Handle("POST", "/webhook", func(ctx context.Context, d webhookhttp.Delivery) error {
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	waitListen(t, addr)

	// Cancel before any traffic. Server should shut down cleanly.
	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("ListenAndServe returned %v after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down within 2s of context cancellation")
	}

	// Listener must be released — a follow-up bind on the same addr should succeed.
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("addr not released after shutdown: %v", err)
	}
	_ = l.Close()
}
