package surface

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeHandler struct {
	dataResp   any
	dataErr    error
	actionResp any
	actionErr  error
}

func (f *fakeHandler) HandleData(_ context.Context, _ DataRequest) (any, error) {
	return f.dataResp, f.dataErr
}
func (f *fakeHandler) HandleAction(_ context.Context, _ ActionRequest) (any, error) {
	return f.actionResp, f.actionErr
}

func TestRegisterHandlers_ServesManifest(t *testing.T) {
	mux := http.NewServeMux()
	m := validManifest()
	RegisterHandlers(mux, m, &fakeHandler{})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/surface/manifest")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var got Manifest
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Surface != "heimdall" {
		t.Errorf("Surface: %q", got.Surface)
	}
}

func TestRegisterHandlers_DataDelegates(t *testing.T) {
	mux := http.NewServeMux()
	h := &fakeHandler{dataResp: []map[string]any{{"id": "p1"}}}
	RegisterHandlers(mux, validManifest(), h)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/surface/data/pulses?status=active")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var got []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0]["id"] != "p1" {
		t.Errorf("rows: %+v", got)
	}
}

func TestRegisterHandlers_ActionDelegates(t *testing.T) {
	mux := http.NewServeMux()
	h := &fakeHandler{actionResp: map[string]any{"ok": true}}
	RegisterHandlers(mux, validManifest(), h)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/surface/action/trigger", "application/json", strings.NewReader(`{"id":"p1"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got["ok"] != true {
		t.Errorf("body: %+v", got)
	}
}
