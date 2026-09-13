package pi

import (
	"encoding/json"
	"testing"
)

// TestToolArgs_DisplayExtraction pins the display-string extraction used in
// tool block headers. For edit tool calls the header shows "path: <path>"
// (issue #2), not the raw JSON — the raw JSON travels separately so the UI
// can render a diff of the replaced blocks.
func TestToolArgs_DisplayExtraction(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{
			name: "bash command",
			args: `{"command":"hostname"}`,
			want: "hostname",
		},
		{
			name: "write content takes precedence over path",
			args: `{"path":"/tmp/f.txt","content":"hello"}`,
			want: "hello",
		},
		{
			name: "edit shows path only",
			args: `{"path":"/tmp/f.py","edits":[{"oldText":"a","newText":"b"}]}`,
			want: "path: /tmp/f.py",
		},
		{
			name: "unknown shape falls back to raw JSON",
			args: `{"pattern":"foo"}`,
			want: `{"pattern":"foo"}`,
		},
		{
			name: "nil args",
			args: "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw json.RawMessage
			if tt.args != "" {
				raw = json.RawMessage(tt.args)
			}
			got := ToolArgs(Event{Args: raw})
			if got != tt.want {
				t.Errorf("ToolArgs() = %q, want %q", got, tt.want)
			}
		})
	}
}
