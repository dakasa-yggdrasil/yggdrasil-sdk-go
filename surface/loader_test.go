package surface

import (
	"os"
	"testing"
)

func TestLoadManifestFromBytes_Heimdall(t *testing.T) {
	raw, err := os.ReadFile("testdata/heimdall_minimal.yaml")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	m, err := LoadManifestFromBytes(raw)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Surface != "heimdall" {
		t.Errorf("Surface: got %q", m.Surface)
	}
	if m.SchemaVersion != 1 {
		t.Errorf("SchemaVersion: got %d", m.SchemaVersion)
	}
	if len(m.Pages) != 1 || m.Pages[0].View.Kind != ViewKindTable {
		t.Errorf("Pages: got %+v", m.Pages)
	}
	if len(m.Pages[0].View.Columns) != 4 {
		t.Errorf("Columns: got %d", len(m.Pages[0].View.Columns))
	}
	if len(m.Permissions) != 1 || m.Permissions[0].ID != "heimdall.pulses.read" {
		t.Errorf("Permissions: got %+v", m.Permissions)
	}
}

func TestLoadManifestFromBytes_RejectsInvalid(t *testing.T) {
	bad := []byte(`surface: ""`)
	if _, err := LoadManifestFromBytes(bad); err == nil {
		t.Fatal("want error for empty surface, got nil")
	}
}
