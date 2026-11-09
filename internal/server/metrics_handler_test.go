package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A failed scrape must keep its status but lose its body: promhttp echoes
// the raw gather error, which can embed a stack trace, and /metrics is
// unauthenticated (issue #260).
func TestQuietScrapesSuppresses5xxBodies(t *testing.T) {
	fail := quietScrapes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gather failed: goroutine 42 [running]: hotelier/...", http.StatusInternalServerError)
	}))

	rec := httptest.NewRecorder()
	fail.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "goroutine") || strings.Contains(body, "hotelier/") {
		t.Errorf("5xx body leaked internal detail: %q", body)
	}
	if !strings.Contains(body, "an error occurred while serving metrics") {
		t.Errorf("5xx body = %q, want the generic line", body)
	}
}

// Everything below 500 — including the healthy exposition — passes through.
func TestQuietScrapesPassesThroughSuccess(t *testing.T) {
	ok := quietScrapes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hotelier_tasks_in_flight 3\n"))
	}))

	rec := httptest.NewRecorder()
	ok.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "hotelier_tasks_in_flight 3\n" {
		t.Errorf("body = %q, want it unchanged", got)
	}
}
