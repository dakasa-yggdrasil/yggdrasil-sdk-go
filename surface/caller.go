package surface

import "strings"

// InputVerifiedCallerID is the canonical top-level key in the on_surface_query
// execute Input envelope carrying the SERVER-VERIFIED caller identity (the
// collaborator UUID) that yggdrasil-core derives from the validated session.
//
// CONTRACT (security-critical):
//   - yggdrasil-core is the ONLY writer. After it validates the session at the
//     surface-query gateway, it stamps this key onto the top-level Input from the
//     session claims — never from client-supplied body/params/input.
//   - Adapters that scope a read by the human caller (e.g. "my-*" self-service
//     views) MUST read THIS field and MUST NOT trust any client-supplied
//     collaborator id nested under Input["params"]. The client controls params;
//     it does NOT control this top-level key.
//   - The field is OPTIONAL on the wire: machine/automation callers
//     (workflow-run-token) authenticate without a collaborator, so the value is
//     empty for them. Adapters decide whether absence is an honest empty or an
//     auth error per their own contract.
//
// Because it travels inside the free-form Input map, adding it is purely
// additive: adapters that don't read it ignore it, and lenient input decoding
// (no json.DisallowUnknownFields) means it can never break decoding anywhere.
const InputVerifiedCallerID = "verified_caller_id"

// VerifiedCallerID extracts the server-verified caller collaborator id from the
// top-level execute Input envelope, returning "" when it is absent, blank, or
// not a string. It deliberately reads only the top-level key (see
// InputVerifiedCallerID) so a client-supplied value nested under params can
// never satisfy it.
func VerifiedCallerID(input map[string]any) string {
	if input == nil {
		return ""
	}
	v, ok := input[InputVerifiedCallerID].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}
