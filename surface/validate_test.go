package surface

import (
	"strings"
	"testing"
)

func validManifest() Manifest {
	return Manifest{
		Surface:        "heimdall",
		SurfaceVersion: "1.0.0",
		SchemaVersion:  1,
		DisplayName:    "Heimdall",
		Icon:           "shield-check",
		Pages: []Page{
			{
				ID:    "pulses",
				Path:  "/pulses",
				Title: "Pulses",
				View:  View{Kind: ViewKindTable, DataSource: &DataSource{Endpoint: "GET /pulses"}},
			},
		},
	}
}

func TestValidate_Accepts_HappyPath(t *testing.T) {
	m := validManifest()
	if err := m.Validate(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidate_RejectsEmptySurface(t *testing.T) {
	m := validManifest()
	m.Surface = ""
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "surface") {
		t.Fatalf("want error mentioning 'surface', got %v", err)
	}
}

func TestValidate_RejectsSchemaVersionZero(t *testing.T) {
	m := validManifest()
	m.SchemaVersion = 0
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("want error mentioning 'schema_version', got %v", err)
	}
}

func TestValidate_RejectsSchemaVersionAboveCurrent(t *testing.T) {
	m := validManifest()
	m.SchemaVersion = SchemaVersionCurrent + 1
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unsupported schema_version") {
		t.Fatalf("want unsupported schema_version error, got %v", err)
	}
}

func TestValidate_RejectsZeroPages(t *testing.T) {
	m := validManifest()
	m.Pages = nil
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "at least one page") {
		t.Fatalf("want 'at least one page' error, got %v", err)
	}
}

func TestValidate_RejectsUnknownViewKind(t *testing.T) {
	m := validManifest()
	m.Pages[0].View.Kind = "lolcat"
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unknown view kind") {
		t.Fatalf("want unknown view kind error, got %v", err)
	}
}

func TestValidate_CustomKindRequiresComponent(t *testing.T) {
	m := validManifest()
	m.Pages[0].View = View{Kind: ViewKindCustom}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "component") {
		t.Fatalf("want component error, got %v", err)
	}
}

func TestValidate_FormKindRequiresFields(t *testing.T) {
	m := validManifest()
	m.Pages[0].View = View{Kind: ViewKindForm}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "requires at least one field") {
		t.Fatalf("want form fields error, got %v", err)
	}
}

func TestValidate_FormKindAcceptsSecretField(t *testing.T) {
	m := validManifest()
	m.Pages[0].View = View{
		Kind: ViewKindForm,
		Fields: []FormField{
			{Field: "token", Label: "Token", Kind: FormFieldKindSecret, Required: true},
		},
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidate_FormKindRejectsUnknownFieldKind(t *testing.T) {
	m := validManifest()
	m.Pages[0].View = View{
		Kind: ViewKindForm,
		Fields: []FormField{
			{Field: "token", Label: "Token", Kind: "password"},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("want unknown field kind error, got %v", err)
	}
}

func TestValidate_DuplicatePageIDsRejected(t *testing.T) {
	m := validManifest()
	m.Pages = append(m.Pages, m.Pages[0])
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate page id") {
		t.Fatalf("want duplicate page id error, got %v", err)
	}
}

func TestValidate_PageRequiresPathStartingWithSlash(t *testing.T) {
	m := validManifest()
	m.Pages[0].Path = "pulses"
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "must start with /") {
		t.Fatalf("want path-must-start-with-slash error, got %v", err)
	}
}
