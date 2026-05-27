package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
)

// ErrMalformedHubSig is returned when X-Hub-Signature-256 lacks the
// expected "sha256=<hex>" shape.
var ErrMalformedHubSig = errors.New("hub sig: malformed header, want sha256=<hex>")

// VerifyHubSignature256 validates a GitHub-style HMAC-SHA256 webhook
// signature (also used by NFe.io and EFI) against payload + secret.
//
// header is the raw "X-Hub-Signature-256" header value, e.g.
//
//	sha256=8cf6e91b...
//
// On success returns nil. On failure returns ErrSignatureMismatch or
// ErrMalformedHubSig.
func VerifyHubSignature256(payload []byte, header string, secret []byte) error {
	const prefix = "sha256="

	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, prefix) {
		return ErrMalformedHubSig
	}
	sigHex := header[len(prefix):]
	if len(sigHex) != sha256.Size*2 {
		return ErrMalformedHubSig
	}
	got, err := hex.DecodeString(sigHex)
	if err != nil {
		return ErrMalformedHubSig
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	expected := mac.Sum(nil)

	if subtle.ConstantTimeCompare(got, expected) != 1 {
		return ErrSignatureMismatch
	}
	return nil
}
