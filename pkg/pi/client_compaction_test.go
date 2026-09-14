package pi

import (
	"encoding/json"
	"testing"
)

// makeCompactionEvent builds an Event from a raw JSON payload, mirroring how
// readEvents unmarshals pi RPC wire lines.
func makeCompactionEvent(t *testing.T, raw string) Event {
	t.Helper()
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("unmarshal event %q: %v", raw, err)
	}
	return e
}

// TestIsCompaction verifies detection of pi compaction events. The wire
// format is documented in pi's rpc.md (compaction_start / compaction_end).
func TestIsCompaction(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{"compaction_start", `{"type":"compaction_start","reason":"threshold"}`, true},
		{"compaction_end", `{"type":"compaction_end","reason":"manual","result":{"summary":"s"}}`, true},
		{"agent_start", `{"type":"agent_start"}`, false},
		{"agent_end", `{"type":"agent_end"}`, false},
		{"agent_settled", `{"type":"agent_settled"}`, false},
		{"tool_execution_start", `{"type":"tool_execution_start","toolCallId":"1","toolName":"bash"}`, false},
		{"message_update", `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"x"}}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCompaction(makeCompactionEvent(t, tt.raw)); got != tt.want {
				t.Errorf("IsCompaction() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCompactionType verifies the start/end classification.
func TestCompactionType(t *testing.T) {
	start := makeCompactionEvent(t, `{"type":"compaction_start","reason":"manual"}`)
	if got := CompactionType(start); got != "start" {
		t.Errorf("CompactionType(start) = %q, want %q", got, "start")
	}

	end := makeCompactionEvent(t, `{"type":"compaction_end","reason":"threshold","result":{"summary":"s"}}`)
	if got := CompactionType(end); got != "end" {
		t.Errorf("CompactionType(end) = %q, want %q", got, "end")
	}
}

// TestCompactionReason verifies extraction of the trigger reason
// (manual, threshold, overflow).
func TestCompactionReason(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"threshold", `{"type":"compaction_start","reason":"threshold"}`, "threshold"},
		{"overflow", `{"type":"compaction_end","reason":"overflow","aborted":true}`, "overflow"},
		{"manual", `{"type":"compaction_start","reason":"manual"}`, "manual"},
		{"missing", `{"type":"compaction_start"}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompactionReason(makeCompactionEvent(t, tt.raw)); got != tt.want {
				t.Errorf("CompactionReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestCompactionSummary verifies extraction of the generated summary from a
// compaction_end result.
func TestCompactionSummary(t *testing.T) {
	raw := `{"type":"compaction_end","reason":"threshold","result":{"summary":"## Goal\nDo the thing.\n\n## Progress\n- Did it.","firstKeptEntryId":"abc","tokensBefore":150000,"estimatedTokensAfter":32000}}`
	if got := CompactionSummary(makeCompactionEvent(t, raw)); got != "## Goal\nDo the thing.\n\n## Progress\n- Did it." {
		t.Errorf("CompactionSummary() = %q, want the summary text", got)
	}

	// No result (e.g. aborted compaction) — empty summary.
	if got := CompactionSummary(makeCompactionEvent(t, `{"type":"compaction_end","reason":"overflow","aborted":true}`)); got != "" {
		t.Errorf("CompactionSummary() = %q, want empty for missing result", got)
	}
}

// TestCompactionTokens verifies extraction of tokensBefore /
// estimatedTokensAfter from a compaction_end result.
func TestCompactionTokens(t *testing.T) {
	raw := `{"type":"compaction_end","reason":"threshold","result":{"summary":"s","tokensBefore":150000,"estimatedTokensAfter":32000}}`
	before, after := CompactionTokens(makeCompactionEvent(t, raw))
	if before != 150000 {
		t.Errorf("tokensBefore = %d, want 150000", before)
	}
	if after != 32000 {
		t.Errorf("estimatedTokensAfter = %d, want 32000", after)
	}

	// No result — zero values.
	before, after = CompactionTokens(makeCompactionEvent(t, `{"type":"compaction_end","reason":"overflow","aborted":true}`))
	if before != 0 || after != 0 {
		t.Errorf("CompactionTokens() = (%d, %d), want (0, 0) for missing result", before, after)
	}
}

// TestCompactionErrorMessage verifies extraction of the error message on a
// failed compaction.
func TestCompactionErrorMessage(t *testing.T) {
	raw := `{"type":"compaction_end","reason":"overflow","aborted":true,"errorMessage":"LLM request failed"}`
	if got := CompactionErrorMessage(makeCompactionEvent(t, raw)); got != "LLM request failed" {
		t.Errorf("CompactionErrorMessage() = %q, want %q", got, "LLM request failed")
	}

	// Successful compaction — no error message.
	if got := CompactionErrorMessage(makeCompactionEvent(t, `{"type":"compaction_end","reason":"threshold","result":{"summary":"s"}}`)); got != "" {
		t.Errorf("CompactionErrorMessage() = %q, want empty for successful compaction", got)
	}
}
