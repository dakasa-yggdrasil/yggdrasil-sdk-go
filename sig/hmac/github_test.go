package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func hubSig(t *testing.T, body, secret []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyHubSignature256_Valid(t *testing.T) {
	secret := []byte("nfeio_webhook_secret")
	body := []byte(`{"event":"issued","id":"abc-123"}`)
	header := hubSig(t, body, secret)

	if err := VerifyHubSignature256(body, header, secret); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestVerifyHubSignature256_TamperedBody(t *testing.T) {
	secret := []byte("nfeio_webhook_secret")
	body := []byte(`{"event":"issued"}`)
	header := hubSig(t, body, secret)

	if err := VerifyHubSignature256([]byte(`{"event":"cancelled"}`), header, secret); !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("expected ErrSignatureMismatch, got %v", err)
	}
}

func TestVerifyHubSignature256_MissingPrefix(t *testing.T) {
	secret := []byte("nfeio_webhook_secret")
	body := []byte(`{"event":"issued"}`)
	// Compute valid hex but omit the "sha256=" prefix.
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	bareHex := hex.EncodeToString(mac.Sum(nil))

	if err := VerifyHubSignature256(body, bareHex, secret); !errors.Is(err, ErrMalformedHubSig) {
		t.Fatalf("expected ErrMalformedHubSig, got %v", err)
	}
}

func TestVerifyHubSignature256_WrongHexLength(t *testing.T) {
	secret := []byte("nfeio_webhook_secret")
	body := []byte(`{"event":"issued"}`)

	if err := VerifyHubSignature256(body, "sha256=deadbeef", secret); !errors.Is(err, ErrMalformedHubSig) {
		t.Fatalf("expected ErrMalformedHubSig for short hex, got %v", err)
	}
}

func TestVerifyHubSignature256_NonHex(t *testing.T) {
	secret := []byte("nfeio_webhook_secret")
	body := []byte(`{"event":"issued"}`)
	bogus := "sha256=" + strings.Repeat("z", 64)

	if err := VerifyHubSignature256(body, bogus, secret); !errors.Is(err, ErrMalformedHubSig) {
		t.Fatalf("expected ErrMalformedHubSig for non-hex chars, got %v", err)
	}
}

func TestVerifyHubSignature256_EmptyHeader(t *testing.T) {
	if err := VerifyHubSignature256([]byte("body"), "", []byte("secret")); !errors.Is(err, ErrMalformedHubSig) {
		t.Fatalf("expected ErrMalformedHubSig for empty header, got %v", err)
	}
}
