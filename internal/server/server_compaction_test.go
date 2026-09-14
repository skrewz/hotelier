package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/rpc"
)

// TestHandleGuestLog_CompactionFields verifies that compaction log entries
// (issue #53) sent via guest.log RPC are passed through with their
// structured fields intact: the in-memory store, the WebSocket notification,
// and the level are all preserved. Compaction entries are non-delta, so they
// are emitted immediately (no accumulator batching).
func TestHandleGuestLog_CompactionFields(t *testing.T) {
	cfg := config.ServerConfig{Host: "127.0.0.1", Port: 0}
	srv := New(cfg)
	hub := srv.Hub()

	go hub.Run()

	conn := rpc.NewTestConnection("browser-conn-compaction", hub)
	hub.Register(conn)
	hub.SetConnectionRole("browser-conn-compaction", rpc.ConnectionRoleBrowser)
	time.Sleep(10 * time.Millisecond)

	// Create a task.
	body, _ := json.Marshal(map[string]interface{}{
		"prompt": "long task",
		"tags":   []string{"business-default"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask struct {
		ID string `json:"id"`
	}
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Send a compaction_end entry with structured fields.
	params := map[string]interface{}{
		"task_id":                  createdTask.ID,
		"line":                     "[COMPACTION_END] (reason: threshold, 150000 -> 32000 tokens)",
		"level":                    "compaction",
		"compaction_type":          "end",
		"compaction_reason":        "threshold",
		"compaction_summary":       "## Goal\nDo the thing.",
		"compaction_tokens_before": 150000,
		"compaction_tokens_after":  32000,
	}
	rawParams, _ := json.Marshal(params)
	if _, rpcErr := hub.Dispatch("guest.log", rawParams); rpcErr != nil {
		t.Fatalf("guest.log dispatch failed: %v", rpcErr)
	}

	// Compaction entries bypass the delta buffer — they must be stored
	// immediately, without a FlushAll.
	logs := srv.LogStore().Get(createdTask.ID)
	if len(logs) != 1 {
		t.Fatalf("expected 1 stored log, got %d", len(logs))
	}
	e := logs[0]
	if e.Level != "compaction" {
		t.Errorf("level = %q, want %q", e.Level, "compaction")
	}
	if e.CompactionType != "end" {
		t.Errorf("CompactionType = %q, want %q", e.CompactionType, "end")
	}
	if e.CompactionReason != "threshold" {
		t.Errorf("CompactionReason = %q, want %q", e.CompactionReason, "threshold")
	}
	if e.CompactionSummary != "## Goal\nDo the thing." {
		t.Errorf("CompactionSummary = %q, want the summary", e.CompactionSummary)
	}
	if e.CompactionTokensBefore != 150000 {
		t.Errorf("CompactionTokensBefore = %d, want 150000", e.CompactionTokensBefore)
	}
	if e.CompactionTokensAfter != 32000 {
		t.Errorf("CompactionTokensAfter = %d, want 32000", e.CompactionTokensAfter)
	}
	if e.CompactionError {
		t.Error("CompactionError = true, want false")
	}

	// The WebSocket notification must carry the same structured fields so
	// the live UI can render the compaction block.
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected task.log notification to be sent to the browser connection")
	}
	var notification rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &notification); err != nil {
		t.Fatalf("failed to unmarshal notification: %v", err)
	}
	if notification.Method != "task.log" {
		t.Fatalf("expected method 'task.log', got %s", notification.Method)
	}
	var notifParams map[string]interface{}
	if err := json.Unmarshal(notification.Params, &notifParams); err != nil {
		t.Fatalf("failed to unmarshal params: %v", err)
	}
	if notifParams["level"] != "compaction" {
		t.Errorf("notification level = %v, want %q", notifParams["level"], "compaction")
	}
	if notifParams["compaction_type"] != "end" {
		t.Errorf("notification compaction_type = %v, want %q", notifParams["compaction_type"], "end")
	}
	if notifParams["compaction_reason"] != "threshold" {
		t.Errorf("notification compaction_reason = %v, want %q", notifParams["compaction_reason"], "threshold")
	}
	if notifParams["compaction_tokens_before"] != float64(150000) {
		t.Errorf("notification compaction_tokens_before = %v, want 150000", notifParams["compaction_tokens_before"])
	}
	if notifParams["compaction_tokens_after"] != float64(32000) {
		t.Errorf("notification compaction_tokens_after = %v, want 32000", notifParams["compaction_tokens_after"])
	}
}
