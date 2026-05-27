package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Error values returned by VerifyStripe. Callers can use errors.Is to
// distinguish — the underlying HTTP layer typically maps every one of
// these to HTTP 400 Bad Request, but the typed error preserves the
// signal for metrics / log fields.
var (
	ErrTimestampExpired  = errors.New("stripe sig: timestamp beyond tolerance window")
	ErrSignatureMismatch = errors.New("stripe sig: v1 HMAC mismatch")
	ErrMissingTimestamp  = errors.New("stripe sig: missing t= component")
	ErrMissingV1         = errors.New("stripe sig: missing v1= component")
	ErrMalformedHeader   = errors.New("stripe sig: malformed header value")
)

// nowFn lets tests inject a deterministic clock. Production paths
// always run time.Now; tests override only inside the package.
var nowFn = time.Now

// VerifyStripe validates a Stripe-Signature header against payload +
// secret, returning the signing timestamp on success.
//
// header is the raw "Stripe-Signature" header value, e.g.
//
//	t=1729012345,v1=8cf...,v0=...
//
// toleranceSecs bounds the |now - ts| skew; pass 300 for the Stripe
// default. A value <= 0 disables the tolerance check entirely and
// rejects only on HMAC mismatch (useful for replay-from-archive
// tests).
func VerifyStripe(payload []byte, header string, secret []byte, toleranceSecs int64) (int64, error) {
	if header == "" {
		return 0, ErrMalformedHeader
	}

	var tsStr string
	var sigs []string
	for _, part := range strings.Split(header, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			tsStr = kv[1]
		case "v1":
			sigs = append(sigs, kv[1])
		}
	}

	if tsStr == "" {
		return 0, ErrMissingTimestamp
	}
	if len(sigs) == 0 {
		return 0, ErrMissingV1
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: parse t=: %v", ErrMalformedHeader, err)
	}

	if toleranceSecs > 0 {
		skew := nowFn().Unix() - ts
		if skew < 0 {
			skew = -skew
		}
		if skew > toleranceSecs {
			return ts, ErrTimestampExpired
		}
	}

	signedPayload := fmt.Sprintf("%d.%s", ts, payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signedPayload))
	expected := mac.Sum(nil)

	for _, sig := range sigs {
		got, err := hex.DecodeString(sig)
		if err != nil {
			continue
		}
		if subtle.ConstantTimeCompare(got, expected) == 1 {
			return ts, nil
		}
	}
	return ts, ErrSignatureMismatch
}
