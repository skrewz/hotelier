package logutil

import "testing"

// TestContent_ReportsSizeNotPayload pins the issue #241 rule: the process log
// records how much content an agentic session produced, never what it said.
func TestContent_ReportsSizeNotPayload(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{"empty", "", "0 bytes"},
		{"ascii", "Read the secret report", "22 bytes"},
		{"multibyte counted in bytes", "sürrealmööser", "16 bytes"},
		{"multiline counted whole", "line one\nline two\n", "18 bytes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Content(tc.text); got != tc.want {
				t.Errorf("Content(%q) = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}

// TestContent_NeverEchoesInput guards against a regression where the helper
// falls back to quoting the text it was handed.
func TestContent_NeverEchoesInput(t *testing.T) {
	secret := "the agent's whole reply, verbatim"
	if got := Content(secret); got != "33 bytes" {
		t.Errorf("Content must not contain the payload, got %q", got)
	}
}
