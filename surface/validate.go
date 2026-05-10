package surface

import (
	"fmt"
	"strings"
)

// Validate checks structural invariants of a Manifest. Errors returned
// here are user-actionable (the integration's manifest.yaml needs a
// fix). Core uses this to reject manifests at fetch time and the
// adapter SDK uses it to fail fast on startup.
func (m *Manifest) Validate() error {
	if strings.TrimSpace(m.Surface) == "" {
		return fmt.Errorf("manifest: surface field is required")
	}
	if m.SchemaVersion <= 0 {
		return fmt.Errorf("manifest: schema_version is required and must be > 0")
	}
	if m.SchemaVersion > SchemaVersionCurrent {
		return fmt.Errorf("manifest: unsupported schema_version %d (this SDK supports up to %d)",
			m.SchemaVersion, SchemaVersionCurrent)
	}
	if strings.TrimSpace(m.SurfaceVersion) == "" {
		return fmt.Errorf("manifest: surface_version is required")
	}
	if strings.TrimSpace(m.DisplayName) == "" {
		return fmt.Errorf("manifest: display_name is required")
	}
	if len(m.Pages) == 0 {
		return fmt.Errorf("manifest: at least one page is required")
	}

	supportedViews := SupportedViewKinds()
	supportedFormFields := SupportedFormFieldKinds()
	pageIDs := make(map[string]bool, len(m.Pages))
	for i := range m.Pages {
		p := &m.Pages[i]
		if strings.TrimSpace(p.ID) == "" {
			return fmt.Errorf("manifest: page[%d] id is required", i)
		}
		if pageIDs[p.ID] {
			return fmt.Errorf("manifest: duplicate page id %q", p.ID)
		}
		pageIDs[p.ID] = true
		if !strings.HasPrefix(p.Path, "/") {
			return fmt.Errorf("manifest: page %q path must start with /", p.ID)
		}
		if strings.TrimSpace(p.Title) == "" {
			return fmt.Errorf("manifest: page %q title is required", p.ID)
		}
		if _, ok := supportedViews[p.View.Kind]; !ok {
			return fmt.Errorf("manifest: page %q has unknown view kind %q", p.ID, p.View.Kind)
		}
		if p.View.Kind == ViewKindCustom && strings.TrimSpace(p.View.Component) == "" {
			return fmt.Errorf("manifest: page %q kind=custom requires a component name", p.ID)
		}
		if p.View.Kind == ViewKindForm {
			if len(p.View.Fields) == 0 {
				return fmt.Errorf("manifest: page %q kind=form requires at least one field", p.ID)
			}
			for j := range p.View.Fields {
				f := &p.View.Fields[j]
				if strings.TrimSpace(f.Field) == "" {
					return fmt.Errorf("manifest: page %q form field[%d] field is required", p.ID, j)
				}
				if strings.TrimSpace(f.Label) == "" {
					return fmt.Errorf("manifest: page %q form field %q label is required", p.ID, f.Field)
				}
				if _, ok := supportedFormFields[f.Kind]; !ok {
					return fmt.Errorf("manifest: page %q form field %q has unknown kind %q", p.ID, f.Field, f.Kind)
				}
				if f.Kind == FormFieldKindSelect && len(f.Options) == 0 {
					return fmt.Errorf("manifest: page %q form field %q kind=select requires options", p.ID, f.Field)
				}
			}
		}
	}

	widgetIDs := make(map[string]bool, len(m.Widgets))
	for i := range m.Widgets {
		w := &m.Widgets[i]
		if strings.TrimSpace(w.ID) == "" {
			return fmt.Errorf("manifest: widget[%d] id is required", i)
		}
		if widgetIDs[w.ID] {
			return fmt.Errorf("manifest: duplicate widget id %q", w.ID)
		}
		widgetIDs[w.ID] = true
		if strings.TrimSpace(w.Target) == "" {
			return fmt.Errorf("manifest: widget %q target is required", w.ID)
		}
		if _, ok := supportedViews[w.View.Kind]; !ok {
			return fmt.Errorf("manifest: widget %q has unknown view kind %q", w.ID, w.View.Kind)
		}
	}

	permIDs := make(map[string]bool, len(m.Permissions))
	for i := range m.Permissions {
		p := &m.Permissions[i]
		if strings.TrimSpace(p.ID) == "" {
			return fmt.Errorf("manifest: permission[%d] id is required", i)
		}
		if permIDs[p.ID] {
			return fmt.Errorf("manifest: duplicate permission id %q", p.ID)
		}
		permIDs[p.ID] = true
	}
	return nil
}
