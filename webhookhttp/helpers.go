package webhookhttp

import (
	"crypto/sha256"
	"strings"

	sdkhmac "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sig/hmac"
)

// ParseHMACSHA256Signature extracts the hex digest from an
// X-Hub-Signature-256 style header value of the form "sha256=<hex>".
// Returns ("", false) when the header is missing the prefix or the
// hex segment is the wrong length.
//
// Caller invokes this helper when it wants to inspect or log the
// signature without verifying — for verification use
// VerifyHMACSHA256Header.
func ParseHMACSHA256Signature(header string) (string, bool) {
	const prefix = "sha256="
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	hexPart := header[len(prefix):]
	if len(hexPart) != sha256.Size*2 {
		return "", false
	}
	return hexPart, true
}

// VerifyHMACSHA256Header verifies a "sha256=<hex>" header against
// payload + secret using constant-time comparison. Thin wrapper over
// sig/hmac.VerifyHubSignature256 — duplicated here only so callers
// of webhookhttp do not have to import a sibling package for the
// 80% case.
func VerifyHMACSHA256Header(body []byte, header string, secret []byte) error {
	return sdkhmac.VerifyHubSignature256(body, header, secret)
}
