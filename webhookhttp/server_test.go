package webhookhttp_test

import (
	"context"
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
