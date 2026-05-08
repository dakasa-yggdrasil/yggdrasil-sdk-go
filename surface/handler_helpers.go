package surface

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// MaxActionBodyBytes caps the action POST body to keep adapters from
// being DoS'd by a misbehaving caller. 512KB is more than enough for
// any reasonable action payload.
const MaxActionBodyBytes = 512 * 1024

// WriteJSON serializes v to w with the given status. Callers are
// expected to NOT call w.WriteHeader/w.Write themselves afterward.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		_, _ = w.Write([]byte("null"))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Already wrote header; best effort to log via response.
		_, _ = fmt.Fprintf(w, `{"error":"encode failed: %s"}`, err.Error())
	}
}

// ReadActionBody reads up to MaxActionBodyBytes of the request body,
// returning an error if the limit is exceeded. The body may be empty.
func ReadActionBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	limited := io.LimitReader(r.Body, MaxActionBodyBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read action body: %w", err)
	}
	if len(raw) > MaxActionBodyBytes {
		return nil, fmt.Errorf("action body exceeds %d bytes", MaxActionBodyBytes)
	}
	return raw, nil
}
