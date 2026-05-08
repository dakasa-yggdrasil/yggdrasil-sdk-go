package surface

import (
	"context"
	"net/http"
)

// DataRequest carries everything HandleData needs without coupling the
// adapter to net/http.
type DataRequest struct {
	ViewID   string
	RawQuery string
	// Header is forwarded read-only — adapters MAY use it for tracing
	// (X-Correlation-Id) but should NOT inspect Authorization (the
	// core has already validated session before proxying).
	Header http.Header
}

// ActionRequest carries everything HandleAction needs.
type ActionRequest struct {
	ActionID string
	Body     []byte // raw JSON body; adapter unmarshals into its own struct
	Header   http.Header
}

// DataHandler is implemented by adapters to serve view data and
// execute actions declared in the manifest. RegisterHandlers wires
// it into a stdlib mux.
type DataHandler interface {
	HandleData(ctx context.Context, req DataRequest) (any, error)
	HandleAction(ctx context.Context, req ActionRequest) (any, error)
}

// RegisterHandlers mounts the three surface endpoints on `mux`:
//
//	GET  /surface/manifest
//	GET  /surface/data/{viewId}
//	POST /surface/action/{actionId}
//
// Use the existing health-server mux of the adapter — there is no
// reason to spin up a second HTTP server for this. The manifest is
// served by value, so any post-construction mutation is ignored.
func RegisterHandlers(mux *http.ServeMux, m Manifest, h DataHandler) {
	mux.HandleFunc("GET /surface/manifest", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, m)
	})
	mux.HandleFunc("GET /surface/data/{viewId}", func(w http.ResponseWriter, r *http.Request) {
		viewID := r.PathValue("viewId")
		if viewID == "" {
			WriteJSON(w, http.StatusBadRequest, errorBody("viewId is required"))
			return
		}
		out, err := h.HandleData(r.Context(), DataRequest{
			ViewID:   viewID,
			RawQuery: r.URL.RawQuery,
			Header:   r.Header,
		})
		if err != nil {
			WriteJSON(w, http.StatusInternalServerError, errorBody(err.Error()))
			return
		}
		WriteJSON(w, http.StatusOK, out)
	})
	mux.HandleFunc("POST /surface/action/{actionId}", func(w http.ResponseWriter, r *http.Request) {
		actionID := r.PathValue("actionId")
		if actionID == "" {
			WriteJSON(w, http.StatusBadRequest, errorBody("actionId is required"))
			return
		}
		body, err := ReadActionBody(r)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, errorBody(err.Error()))
			return
		}
		out, err := h.HandleAction(r.Context(), ActionRequest{
			ActionID: actionID,
			Body:     body,
			Header:   r.Header,
		})
		if err != nil {
			WriteJSON(w, http.StatusInternalServerError, errorBody(err.Error()))
			return
		}
		WriteJSON(w, http.StatusOK, out)
	})
}

func errorBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
