package surface

// Supported view kinds. The renderer in console-ops switches on these.
const (
	ViewKindTable         = "table"
	ViewKindDetail        = "detail"
	ViewKindForm          = "form"
	ViewKindTimeline      = "timeline"
	ViewKindStatusGrid    = "status-grid"
	ViewKindJSONInspector = "json-inspector"
	ViewKindChart         = "chart"
	ViewKindEmbed         = "embed"
	ViewKindCustom        = "custom"
	ViewKindStatCard      = "stat-card"           // widget-only
	ViewKindFilterContrib = "filter-contribution" // widget-only
)

// Supported field formats.
const (
	FormatText        = "text"
	FormatCode        = "code"
	FormatCodeBlockMD = "code-block-md"
	FormatTimeago     = "timeago"
	FormatDatetime    = "datetime"
	FormatCurrency    = "currency"
	FormatNumber      = "number"
	FormatPercent     = "percent"
	FormatBadge       = "badge"
	FormatLink        = "link"
	FormatJSON        = "json"
	FormatBytes       = "bytes"
	FormatDuration    = "duration"
)

// Supported filter kinds.
const (
	FilterKindSearch       = "search"
	FilterKindChipMulti    = "chip-multi"
	FilterKindChipToggle   = "chip-toggle"
	FilterKindSelect       = "select"
	FilterKindDateRange    = "date-range"
	FilterKindNumberRange  = "number-range"
	FilterKindContribution = "filter-contribution"
)

// Slot targets for widgets.
const (
	SlotOpsHome             = "ops_home"
	SlotOpsWorkflowsFilters = "ops_workflows.filters"
	SlotOpsIntegrationsTabs = "ops_integrations.detail.tabs"
	SlotOpsAuditFilters     = "ops_audit.filters"
	SlotOpsCatalogTabs      = "ops_catalog.tabs"
)

// SchemaVersionCurrent is the version this SDK release implements. Core
// MUST accept manifests with a SchemaVersion <= SchemaVersionCurrent.
const SchemaVersionCurrent = 1

// SupportedViewKinds returns the set of view kinds the validator will
// accept. Kept as a function (not var) so callers can range copy-free.
func SupportedViewKinds() map[string]struct{} {
	return map[string]struct{}{
		ViewKindTable:         {},
		ViewKindDetail:        {},
		ViewKindForm:          {},
		ViewKindTimeline:      {},
		ViewKindStatusGrid:    {},
		ViewKindJSONInspector: {},
		ViewKindChart:         {},
		ViewKindEmbed:         {},
		ViewKindCustom:        {},
		ViewKindStatCard:      {},
		ViewKindFilterContrib: {},
	}
}
