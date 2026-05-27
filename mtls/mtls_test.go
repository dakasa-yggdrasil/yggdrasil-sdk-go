package mtls

import (
	"encoding/base64"
	"os"
	"testing"
)

func TestLoad_FromFile(t *testing.T) {
	cfg := Config{
		Source: SourceFile,
		Path:   "testdata/client-cert.p12",
	}
	tlsCfg, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil *tls.Config")
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
	if tlsCfg.Certificates[0].PrivateKey == nil {
		t.Fatal("expected non-nil PrivateKey on the certificate")
	}
}

func TestLoad_FromFileWithPassword(t *testing.T) {
	cfg := Config{
		Source:   SourceFile,
		Path:     "testdata/client-cert-pwd.p12",
		Password: "secret",
	}
	tlsCfg, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
}

func TestLoad_FromFileWrongPassword(t *testing.T) {
	cfg := Config{
		Source:   SourceFile,
		Path:     "testdata/client-cert-pwd.p12",
		Password: "wrong-password",
	}
	_, err := Load(cfg)
	if err == nil {
		t.Fatal("expected error for wrong password, got nil")
	}
}

func TestLoad_FromBase64(t *testing.T) {
	raw, err := os.ReadFile("testdata/client-cert.p12")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(raw)

	tlsCfg, err := Load(Config{
		Source: SourceBase64,
		Base64: encoded,
	})
	if err != nil {
		t.Fatalf("Load(SourceBase64) failed: %v", err)
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 cert, got %d", len(tlsCfg.Certificates))
	}
}

func TestLoad_FromBase64Invalid(t *testing.T) {
	_, err := Load(Config{
		Source: SourceBase64,
		Base64: "!!!not-base64!!!",
	})
	if err == nil {
		t.Fatal("expected error for malformed base64, got nil")
	}
}

func TestLoad_FromFileMissing(t *testing.T) {
	_, err := Load(Config{
		Source: SourceFile,
		Path:   "testdata/does-not-exist.p12",
	})
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoad_FromFileMalformedP12(t *testing.T) {
	// Write a junk file then try to load it.
	tmp := t.TempDir() + "/junk.p12"
	if err := os.WriteFile(tmp, []byte("not a p12 bundle"), 0o600); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	_, err := Load(Config{Source: SourceFile, Path: tmp})
	if err == nil {
		t.Fatal("expected pkcs12 decode error, got nil")
	}
}
