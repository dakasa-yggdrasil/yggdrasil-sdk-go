package mtls

import (
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
