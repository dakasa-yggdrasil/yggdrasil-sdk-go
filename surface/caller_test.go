package surface

import "testing"

func TestVerifiedCallerID_Present(t *testing.T) {
	input := map[string]any{
		"query_name":          "my-employment",
		InputVerifiedCallerID: "11111111-1111-1111-1111-111111111111",
		"params":              map[string]any{"collaborator_id": "spoofed"},
	}
	got := VerifiedCallerID(input)
	if got != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("VerifiedCallerID: got %q", got)
	}
}

func TestVerifiedCallerID_TrimsWhitespace(t *testing.T) {
	input := map[string]any{InputVerifiedCallerID: "  abc  "}
	if got := VerifiedCallerID(input); got != "abc" {
		t.Fatalf("VerifiedCallerID trim: got %q", got)
	}
}

func TestVerifiedCallerID_AbsentOrWrongType(t *testing.T) {
	cases := []map[string]any{
		nil,
		{},
		{InputVerifiedCallerID: ""},
		{InputVerifiedCallerID: 123},      // wrong type
		{InputVerifiedCallerID: "   "},    // whitespace only
		{"verified_caller": "almost"},      // wrong key
	}
	for i, c := range cases {
		if got := VerifiedCallerID(c); got != "" {
			t.Errorf("case %d: expected empty, got %q", i, got)
		}
	}
}

// The contract constant must be the exact wire key core stamps and adapters read.
func TestInputVerifiedCallerID_WireKey(t *testing.T) {
	if InputVerifiedCallerID != "verified_caller_id" {
		t.Fatalf("wire key drift: %q", InputVerifiedCallerID)
	}
}
