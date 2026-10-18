package executorpower

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPAuthority struct {
	base, token string
	client      *http.Client
}

func NewHTTPAuthority(base, existingToken string, client *http.Client) (*HTTPAuthority, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" || strings.TrimSpace(existingToken) != existingToken || len(existingToken) < 32 || len(existingToken) > 4096 {
		return nil, ErrDenied
	}
	if client == nil {
		client = &http.Client{}
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &HTTPAuthority{base: strings.TrimRight(base, "/"), token: existingToken, client: &c}, nil
}
func (a *HTTPAuthority) post(ctx context.Context, path string, payload any, token string, out any) error {
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > 32768 {
		return ErrDenied
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/api/v1/capacity/mutations/power/"+path, bytes.NewReader(raw))
	if err != nil {
		return ErrDenied
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Capacity-Mutation-Settlement", token)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return ErrUnknown
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%w: authority response", ErrUnknown)
	}
	raw, err = io.ReadAll(io.LimitReader(resp.Body, 32769))
	if err != nil || len(raw) > 32768 {
		return ErrUnknown
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return ErrUnknown
	}
	return nil
}
func (a *HTTPAuthority) Redeem(ctx context.Context, integration string, c Claim) (ClaimReply, error) {
	var r ClaimReply
	err := a.post(ctx, "redeem", struct {
		Integration string `json:"integration_instance_id"`
		Action      string `json:"action"`
		Claim       Claim  `json:"claim"`
	}{integration, c.Action, c}, "", &r)
	return r, err
}
func (a *HTTPAuthority) Settle(ctx context.Context, integration, action, token string, r Result) error {
	var out struct {
		Recorded bool `json:"recorded"`
	}
	err := a.post(ctx, "settle", struct {
		Integration string `json:"integration_instance_id"`
		Action      string `json:"action"`
		Result      Result `json:"result"`
	}{integration, action, r}, token, &out)
	if err != nil {
		return err
	}
	if !out.Recorded {
		return ErrUnknown
	}
	return nil
}
