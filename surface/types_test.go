package surface

import (
	"encoding/json"
	"testing"
)

func TestManifestJSONRoundTrip(t *testing.T) {
	m := Manifest{
		Surface:        "heimdall",
		SurfaceVersion: "1.0.0",
		SchemaVersion:  1,
		DisplayName:    "Heimdall",
		Icon:           "shield-check",
		Description:    "Operador autônomo do cluster",
		Permissions: []Permission{
			{ID: "heimdall.pulses.read", Label: "Ver pulses"},
		},
		Pages: []Page{
			{
				ID:    "pulses",
				Path:  "/pulses",
				Title: "Pulses",
				View:  View{Kind: "table"},
			},
		},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Surface != m.Surface {
		t.Errorf("Surface: got %q want %q", got.Surface, m.Surface)
	}
	if got.SchemaVersion != m.SchemaVersion {
		t.Errorf("SchemaVersion: got %d want %d", got.SchemaVersion, m.SchemaVersion)
	}
	if len(got.Pages) != 1 || got.Pages[0].ID != "pulses" {
		t.Errorf("Pages: got %+v", got.Pages)
	}
	if len(got.Permissions) != 1 || got.Permissions[0].ID != "heimdall.pulses.read" {
		t.Errorf("Permissions: got %+v", got.Permissions)
	}
}

func TestFormFieldsJSONRoundTrip(t *testing.T) {
	m := validManifest()
	m.Pages = append(m.Pages, Page{
		ID:    "configure",
		Path:  "/configure",
		Title: "Configuração",
		View: View{
			Kind:        ViewKindForm,
			Endpoint:    "POST /configure",
			Method:      "POST",
			SubmitLabel: "Salvar Configuração",
			Fields: []FormField{
				{Field: "credentials_ref", Label: "Secret Reference", Kind: FormFieldKindString, Required: true},
				{Field: "token", Label: "Token", Kind: FormFieldKindSecret},
			},
		},
	})

	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	form := got.Pages[1].View
	if form.Kind != ViewKindForm || form.Endpoint != "POST /configure" || form.SubmitLabel == "" {
		t.Fatalf("form view not preserved: %+v", form)
	}
	if len(form.Fields) != 2 || form.Fields[1].Kind != FormFieldKindSecret {
		t.Fatalf("fields not preserved: %+v", form.Fields)
	}
}
