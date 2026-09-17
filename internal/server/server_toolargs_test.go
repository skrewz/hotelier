package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// TestHandleGuestLog_ToolArgsJSONPassthrough verifies that the raw tool
// argument JSON (tool_args_json) sent by the guest is preserved in the
// stored log entry, returned by the task detail API, and broadcast in the
// task.log WebSocket notification. The UI uses it to render a diff for
// edit tool calls (issue #2).
func TestHandleGuestLog_ToolArgsJSONPassthrough(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a browser connection to observe the task.log notification.
	conn := rpc.NewTestConnection("browser-conn-1", hub)
	hub.Register(conn)
	hub.SetConnectionRole("browser-conn-1", rpc.ConnectionRoleBrowser)
	time.Sleep(10 * time.Millisecond)

	// Create a task.
	body, _ := json.Marshal(map[string]interface{}{
		"prompt": "edit args json test",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	const argsJSON = `{"path":"/tmp/example-file.py","edits":[{"oldText":"a","newText":"b"}]}`

	// Send a tool start entry with the raw args JSON.
	params := map[string]interface{}{
		"task_id":        createdTask.ID,
		"line":           "[TOOL_START] edit: path: /tmp/example-file.py (id: call-1)",
		"level":          "tool",
		"tool_type":      "start",
		"tool_name":      "edit",
		"tool_id":        "call-1",
		"tool_args":      "path: /tmp/example-file.py",
		"tool_args_json": argsJSON,
	}
	rawParams, _ := json.Marshal(params)
	hub.Dispatch("guest.log", rawParams)

	// Tool entries bypass the accumulator, so the entry is stored
	// synchronously by the time Dispatch returns.
	logs := srv.LogStore().Get(createdTask.ID)
	if len(logs) != 1 {
		t.Fatalf("expected 1 stored log, got %d", len(logs))
	}
	if logs[0].ToolArgsJSON != argsJSON {
		t.Errorf("stored entry: expected tool_args_json %q, got %q", argsJSON, logs[0].ToolArgsJSON)
	}

	// The task detail API must return the field.
	req2 := httptest.NewRequest(http.MethodGet, "/api/tasks/"+createdTask.ID, nil)
	w2 := httptest.NewRecorder()
	srv.HandleTaskDetail(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}
	var detail map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to unmarshal detail: %v", err)
	}
	apiLogs, ok := detail["logs"].([]interface{})
	if !ok || len(apiLogs) != 1 {
		t.Fatalf("expected 1 log in detail response, got %v", detail["logs"])
	}
	apiEntry, ok := apiLogs[0].(map[string]interface{})
	if !ok {
		t.Fatalf("log entry: expected map, got %T", apiLogs[0])
	}
	if apiEntry["tool_args_json"] != argsJSON {
		t.Errorf("detail API: expected tool_args_json %q, got %v", argsJSON, apiEntry["tool_args_json"])
	}

	// The WebSocket notification must carry the field too (streaming path).
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
	if notifParams["tool_args_json"] != argsJSON {
		t.Errorf("notification: expected tool_args_json %q, got %v", argsJSON, notifParams["tool_args_json"])
	}
}
