package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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

func TestVerifyStripe_TamperedBodyRejected(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1","type":"payment_intent.succeeded"}`)
	now := time.Now().Unix()
	header := stripeSig(t, now, body, secret)

	tampered := []byte(`{"id":"evt_1","type":"payment_intent.succeeded","amount":999}`)
	_, err := VerifyStripe(tampered, header, secret, 300)
	if err == nil {
		t.Fatal("expected error for tampered body, got nil")
	}
	if !errorsIs(err, ErrSignatureMismatch) {
		t.Fatalf("expected ErrSignatureMismatch, got %v", err)
	}
}

func TestVerifyStripe_TamperedSignatureRejected(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Now().Unix()
	// Flip one hex char in the v1 component.
	header := stripeSig(t, now, body, secret)
	flipped := header[:len(header)-1] + "0"
	if flipped[len(flipped)-1] == header[len(header)-1] {
		flipped = header[:len(header)-1] + "1"
	}

	_, err := VerifyStripe(body, flipped, secret, 300)
	if !errorsIs(err, ErrSignatureMismatch) {
		t.Fatalf("expected ErrSignatureMismatch, got %v", err)
	}
}

// errorsIs is a local alias so the test file imports stay minimal.
func errorsIs(err, target error) bool { return errors.Is(err, target) }

func TestVerifyStripe_ExpiredTimestamp(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1"}`)
	old := time.Now().Unix() - 600 // 10 minutes ago, tolerance 300s

	header := stripeSig(t, old, body, secret)

	ts, err := VerifyStripe(body, header, secret, 300)
	if !errors.Is(err, ErrTimestampExpired) {
		t.Fatalf("expected ErrTimestampExpired, got %v", err)
	}
	if ts != old {
		t.Fatalf("expected ts to be parsed even on tolerance failure; got %d want %d", ts, old)
	}
}

func TestVerifyStripe_FutureTimestamp(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1"}`)
	future := time.Now().Unix() + 600

	header := stripeSig(t, future, body, secret)

	_, err := VerifyStripe(body, header, secret, 300)
	if !errors.Is(err, ErrTimestampExpired) {
		t.Fatalf("expected ErrTimestampExpired for future timestamp, got %v", err)
	}
}

func TestVerifyStripe_ToleranceZeroSkipsTimestampCheck(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1"}`)
	veryOld := time.Now().Unix() - 86400 // 1 day ago

	header := stripeSig(t, veryOld, body, secret)

	ts, err := VerifyStripe(body, header, secret, 0)
	if err != nil {
		t.Fatalf("tolerance=0 should skip timestamp check, got %v", err)
	}
	if ts != veryOld {
		t.Fatalf("expected ts=%d, got %d", veryOld, ts)
	}
}

func TestVerifyStripe_MissingTimestamp(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1"}`)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("0." + string(body)))
	header := "v1=" + hex.EncodeToString(mac.Sum(nil))

	_, err := VerifyStripe(body, header, secret, 300)
	if !errors.Is(err, ErrMissingTimestamp) {
		t.Fatalf("expected ErrMissingTimestamp, got %v", err)
	}
}

func TestVerifyStripe_MissingV1(t *testing.T) {
	secret := []byte("whsec_test_abcdef")
	body := []byte(`{"id":"evt_1"}`)
	header := fmt.Sprintf("t=%d,v0=abcdef", time.Now().Unix())

	_, err := VerifyStripe(body, header, secret, 300)
	if !errors.Is(err, ErrMissingV1) {
		t.Fatalf("expected ErrMissingV1, got %v", err)
	}
}

func TestVerifyStripe_EmptyHeader(t *testing.T) {
	_, err := VerifyStripe([]byte("body"), "", []byte("secret"), 300)
	if !errors.Is(err, ErrMalformedHeader) {
		t.Fatalf("expected ErrMalformedHeader for empty header, got %v", err)
	}
}

func TestVerifyStripe_MultipleV1ComponentsFirstMatch(t *testing.T) {
	// Stripe rotates signing keys by emitting multiple v1= components,
	// signed with old + new secrets. Verifier must accept the request
	// if ANY component matches the secret in use.
	secret := []byte("whsec_new")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Now().Unix()
	validSig := stripeSig(t, now, body, secret)
	// Build "t=<n>,v1=<bogus>,v1=<valid>" by stitching.
	parts := strings.Split(validSig, ",")
	merged := parts[0] + ",v1=deadbeef," + parts[1]

	_, err := VerifyStripe(body, merged, secret, 300)
	if err != nil {
		t.Fatalf("expected success with at least one matching v1, got %v", err)
	}
}
