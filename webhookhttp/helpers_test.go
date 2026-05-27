package webhookhttp_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/webhookhttp"
)

func TestParseHMACSHA256Signature_Valid(t *testing.T) {
	got, ok := webhookhttp.ParseHMACSHA256Signature("sha256=deadbeef" + strings.Repeat("0", 56))
	if !ok {
		t.Fatal("expected ok=true on well-formed header")
	}
	if got != "deadbeef"+strings.Repeat("0", 56) {
		t.Fatalf("hex mismatch: %q", got)
	}
}

func TestParseHMACSHA256Signature_MissingPrefix(t *testing.T) {
	_, ok := webhookhttp.ParseHMACSHA256Signature(strings.Repeat("0", 64))
	if ok {
		t.Fatal("expected ok=false without sha256= prefix")
	}
}

func TestParseHMACSHA256Signature_Empty(t *testing.T) {
	_, ok := webhookhttp.ParseHMACSHA256Signature("")
	if ok {
		t.Fatal("expected ok=false on empty header")
	}
}

func TestVerifyHMACSHA256Header_ConstantTimeValid(t *testing.T) {
	secret := []byte("topsecret")
	body := []byte(`{"a":1}`)
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	header := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if err := webhookhttp.VerifyHMACSHA256Header(body, header, secret); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestVerifyHMACSHA256Header_Mismatch(t *testing.T) {
	secret := []byte("topsecret")
	body := []byte(`{"a":1}`)
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	header := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	tampered := []byte(`{"a":2}`)
	if err := webhookhttp.VerifyHMACSHA256Header(tampered, header, secret); err == nil {
		t.Fatal("expected mismatch error on tampered body")
	}
}
