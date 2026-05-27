package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

// stripeSig produces a valid Stripe-Signature header value for tests,
// matching the format Stripe uses on real deliveries:
//
//	t=<unix>,v1=<hex(HMAC_SHA256(secret, "<unix>.<body>"))>
func stripeSig(t *testing.T, ts int64, body []byte, secret []byte) string {
	t.Helper()
	payload := fmt.Sprintf("%d.%s", ts, body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}

func TestVerifyStripe_ValidSignature(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1","type":"payment_intent.succeeded"}`)
	now := time.Now().Unix()
	header := stripeSig(t, now, body, secret)

	ts, err := VerifyStripe(body, header, secret, 300)
	if err != nil {
		t.Fatalf("VerifyStripe returned error: %v", err)
	}
	if ts != now {
		t.Fatalf("expected ts=%d, got %d", now, ts)
	}
}
