package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
