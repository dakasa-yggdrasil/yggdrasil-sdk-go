// Package surface defines the contract between yggdrasil-core and
// integration adapters that contribute pages/widgets to console-ops.
//
// An adapter declares a static Manifest (typically loaded from
// surface/manifest.yaml), implements DataHandler to serve view data
// and execute actions, and calls RegisterHandlers to mount the three
// HTTP endpoints on its existing health server mux.
package surface

// Manifest is the root document an adapter exposes via
// GET /surface/manifest. It tells the core which pages, widgets and
// permissions this surface contributes to console-ops.
type Manifest struct {
	// Surface is the surface id; must equal the integration id.
	Surface string `json:"surface" yaml:"surface"`

	// SurfaceVersion is the semver of the surface UI shipped by the
	// adapter binary. Bumps on every UI change.
	SurfaceVersion string `json:"surface_version" yaml:"surface_version"`

	// SchemaVersion is the integer version of the manifest contract
	// itself. Core rejects manifests with a SchemaVersion outside the
	// supported range.
	SchemaVersion int `json:"schema_version" yaml:"schema_version"`

	DisplayName string `json:"display_name" yaml:"display_name"`
	Icon        string `json:"icon" yaml:"icon"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`

	Permissions []Permission `json:"permissions,omitempty" yaml:"permissions,omitempty"`
	Pages       []Page       `json:"pages" yaml:"pages"`
	Widgets     []Widget     `json:"widgets,omitempty" yaml:"widgets,omitempty"`
}

// Permission is a single RBAC permission this surface introduces.
// Core reconciles these into the global permissions_catalog table.
type Permission struct {
	ID    string `json:"id" yaml:"id"`
	Label string `json:"label" yaml:"label"`
}

// Page is a sub-route inside /ops/integrations/<surface.id>/<page.path>.
type Page struct {
	ID         string   `json:"id" yaml:"id"`
	Path       string   `json:"path" yaml:"path"`
	Title      string   `json:"title" yaml:"title"`
	Requires   []string `json:"requires,omitempty" yaml:"requires,omitempty"`
	View       View     `json:"view" yaml:"view"`
	EmptyState *Empty   `json:"empty_state,omitempty" yaml:"empty_state,omitempty"`
}

// Widget is a contribution to a slot inside a console-ops core page
// (ops_home, ops_workflows.filters, ops_integrations.detail.tabs,
// ops_audit.filters, ops_catalog.tabs).
type Widget struct {
	ID       string `json:"id" yaml:"id"`
	Target   string `json:"target" yaml:"target"`
	Section  string `json:"section,omitempty" yaml:"section,omitempty"`
	Priority int    `json:"priority,omitempty" yaml:"priority,omitempty"`
	View     View   `json:"view" yaml:"view"`
}

// Empty is the empty-state shown when the data source returns 0 rows.
type Empty struct {
	Title string `json:"title" yaml:"title"`
	Body  string `json:"body" yaml:"body"`
}

// DataSource locates the data behind a view/widget. Endpoint is
// resolved by the core proxy against /api/v1/ops/surfaces/:id/data.
type DataSource struct {
	Endpoint         string `json:"endpoint" yaml:"endpoint"`
	RefreshIntervalS int    `json:"refresh_interval_s,omitempty" yaml:"refresh_interval_s,omitempty"`
}

// View is the renderable definition. Kind selects the renderer in the
// console; the rest of the fields are kind-specific. Unknown fields
// are tolerated for forward compatibility.
type View struct {
	Kind        string                 `json:"kind" yaml:"kind"`
	Component   string                 `json:"component,omitempty" yaml:"component,omitempty"`
	DataSource  *DataSource            `json:"data_source,omitempty" yaml:"data_source,omitempty"`
	Columns     []Column               `json:"columns,omitempty" yaml:"columns,omitempty"`
	Filters     []Filter               `json:"filters,omitempty" yaml:"filters,omitempty"`
	RowActions  []Action               `json:"row_actions,omitempty" yaml:"row_actions,omitempty"`
	Item        *TimelineItem          `json:"item,omitempty" yaml:"item,omitempty"`
	Drawers     []Drawer               `json:"drawers,omitempty" yaml:"drawers,omitempty"`
	Sections    []DetailSection        `json:"sections,omitempty" yaml:"sections,omitempty"`
	Metrics     []Metric               `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	Endpoint    string                 `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	Method      string                 `json:"method,omitempty" yaml:"method,omitempty"`
	Fields      []FormField            `json:"fields,omitempty" yaml:"fields,omitempty"`
	SubmitLabel string                 `json:"submit_label,omitempty" yaml:"submit_label,omitempty"`
	Title       string                 `json:"title,omitempty" yaml:"title,omitempty"`
	Link        string                 `json:"link,omitempty" yaml:"link,omitempty"`
	URLField    string                 `json:"url_field,omitempty" yaml:"url_field,omitempty"`
	Filter      *Filter                `json:"filter,omitempty" yaml:"filter,omitempty"`
	Extras      map[string]interface{} `json:"extras,omitempty" yaml:"extras,omitempty"`
}

// Column describes one column of a table view.
type Column struct {
	Field  string `json:"field" yaml:"field"`
	Label  string `json:"label" yaml:"label"`
	Format string `json:"format,omitempty" yaml:"format,omitempty"`
	Suffix string `json:"suffix,omitempty" yaml:"suffix,omitempty"`
}

// Filter is a UI filter control.
type Filter struct {
	ID          string                 `json:"id" yaml:"id"`
	Kind        string                 `json:"kind" yaml:"kind"`
	Field       string                 `json:"field,omitempty" yaml:"field,omitempty"`
	Label       string                 `json:"label,omitempty" yaml:"label,omitempty"`
	Placeholder string                 `json:"placeholder,omitempty" yaml:"placeholder,omitempty"`
	Options     []string               `json:"options,omitempty" yaml:"options,omitempty"`
	Applies     map[string]interface{} `json:"applies,omitempty" yaml:"applies,omitempty"`
}

// FormField describes one field rendered by a form view. Kind "secret"
// is intentionally separate from "string" so consoles can use password
// inputs and keep values out of casual screenshots.
type FormField struct {
	Field    string      `json:"field" yaml:"field"`
	Label    string      `json:"label" yaml:"label"`
	Kind     string      `json:"kind" yaml:"kind"`
	Required bool        `json:"required,omitempty" yaml:"required,omitempty"`
	Options  []string    `json:"options,omitempty" yaml:"options,omitempty"`
	Default  interface{} `json:"default,omitempty" yaml:"default,omitempty"`
}

// Action describes a button bound to POST /surface/action/<id>.
type Action struct {
	ID        string                 `json:"id" yaml:"id"`
	Label     string                 `json:"label" yaml:"label"`
	Endpoint  string                 `json:"endpoint" yaml:"endpoint"`
	Requires  []string               `json:"requires,omitempty" yaml:"requires,omitempty"`
	Confirm   *Confirm               `json:"confirm,omitempty" yaml:"confirm,omitempty"`
	OnSuccess map[string]interface{} `json:"on_success,omitempty" yaml:"on_success,omitempty"`
	ShowWhen  string                 `json:"show_when,omitempty" yaml:"show_when,omitempty"`
}

// Confirm is the modal shown before an Action runs.
type Confirm struct {
	Title string `json:"title" yaml:"title"`
	Body  string `json:"body" yaml:"body"`
}

// Drawer is a side-panel opened from a timeline/table row.
type Drawer struct {
	ID    string `json:"id" yaml:"id"`
	Fetch string `json:"fetch" yaml:"fetch"`
	View  View   `json:"view" yaml:"view"`
}

// TimelineItem describes how to render one timeline entry.
type TimelineItem struct {
	TimestampField string                 `json:"timestamp_field" yaml:"timestamp_field"`
	TitleField     string                 `json:"title_field" yaml:"title_field"`
	BodyField      string                 `json:"body_field,omitempty" yaml:"body_field,omitempty"`
	Badges         []Column               `json:"badges,omitempty" yaml:"badges,omitempty"`
	Click          map[string]interface{} `json:"click,omitempty" yaml:"click,omitempty"`
}

// DetailSection groups fields inside a detail view.
type DetailSection struct {
	Title  string  `json:"title" yaml:"title"`
	Fields []Field `json:"fields" yaml:"fields"`
}

// Field is one labeled value inside a detail section.
type Field struct {
	Field  string `json:"field" yaml:"field"`
	Label  string `json:"label,omitempty" yaml:"label,omitempty"`
	Format string `json:"format,omitempty" yaml:"format,omitempty"`
}

// Metric describes one stat shown inside a stat-card widget.
type Metric struct {
	Label     string `json:"label" yaml:"label"`
	Field     string `json:"field" yaml:"field"`
	AlertWhen string `json:"alert_when,omitempty" yaml:"alert_when,omitempty"`
}
