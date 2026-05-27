package mtls

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"golang.org/x/crypto/pkcs12"
)

// Source selects how the cert+key material is supplied to Load.
type Source int

const (
	// SourceDisabled means mTLS is opted out (e.g. feature-flagged
	// mock mode). Load returns (nil, nil) so callers can construct
	// their HTTP client without TLS configuration.
	SourceDisabled Source = iota

	// SourceFile reads a PKCS#12 bundle from Config.Path.
	SourceFile

	// SourceBase64 decodes Config.Base64 (no padding tolerance — base64
	// per stdlib encoding/base64).
	SourceBase64
)

// Config selects the source and supplies the source-specific values.
type Config struct {
	Source   Source
	Path     string // SourceFile
	Base64   string // SourceBase64
	Password string // optional; empty means no password
}

// Load builds a *tls.Config from cfg suitable for use as the
// TLSClientConfig on an http.Transport (outbound mTLS) and as the
// TLSConfig on an http.Server (inbound mTLS with ClientCAs set by
// the caller).
//
// SourceDisabled returns (nil, nil) by design — let callers treat
// nil as "construct a plain HTTP client".
func Load(cfg Config) (*tls.Config, error) {
	switch cfg.Source {
	case SourceDisabled:
		return nil, nil
	case SourceFile:
		raw, err := os.ReadFile(cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("mtls: read %q: %w", cfg.Path, err)
		}
		return loadFromP12Bytes(raw, cfg.Password)
	case SourceBase64:
		raw, err := base64.StdEncoding.DecodeString(cfg.Base64)
		if err != nil {
			return nil, fmt.Errorf("mtls: base64 decode: %w", err)
		}
		return loadFromP12Bytes(raw, cfg.Password)
	default:
		return nil, fmt.Errorf("mtls: unknown Source value %d", cfg.Source)
	}
}

// LoadFromEnv builds a Config from environment variables and calls
// Load. The variable names are derived from a prefix:
//
//	{PREFIX}_MTLS_ENABLED        — "false" / "0" / "" → SourceDisabled
//	                               anything else      → continue
//	{PREFIX}_CERTIFICATE         — when non-empty,    → SourceFile
//	{PREFIX}_CERTIFICATE_BASE64  — when non-empty,    → SourceBase64
//	{PREFIX}_CERTIFICATE_PASSWORD — optional, used by SourceFile/Base64
//
// {PREFIX}_CERTIFICATE takes precedence over the base64 variant when
// both are set.
//
// Adapter convention: integration-efi uses prefix "EFI",
// integration-stripe uses prefix "STRIPE", and so on.
func LoadFromEnv(prefix string) (*tls.Config, error) {
	enabled := os.Getenv(prefix + "_MTLS_ENABLED")
	switch enabled {
	case "", "false", "0", "False", "FALSE":
		return Load(Config{Source: SourceDisabled})
	}

	path := os.Getenv(prefix + "_CERTIFICATE")
	b64 := os.Getenv(prefix + "_CERTIFICATE_BASE64")
	pwd := os.Getenv(prefix + "_CERTIFICATE_PASSWORD")

	switch {
	case path != "":
		return Load(Config{Source: SourceFile, Path: path, Password: pwd})
	case b64 != "":
		return Load(Config{Source: SourceBase64, Base64: b64, Password: pwd})
	default:
		return nil, fmt.Errorf("mtls: %s_MTLS_ENABLED=true but neither %s_CERTIFICATE nor %s_CERTIFICATE_BASE64 is set", prefix, prefix, prefix)
	}
}

func loadFromP12Bytes(raw []byte, password string) (*tls.Config, error) {
	if len(raw) == 0 {
		return nil, errors.New("mtls: empty P12 payload")
	}
	key, cert, err := pkcs12.Decode(raw, password)
	if err != nil {
		return nil, fmt.Errorf("mtls: pkcs12 decode: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("mtls: P12 contained unsupported key type %T (want *rsa.PrivateKey)", key)
	}
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert.Raw},
		PrivateKey:  rsaKey,
		Leaf:        cert,
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		RootCAs:      x509.NewCertPool(), // caller may populate; package default is empty
	}, nil
}
