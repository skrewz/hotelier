package server

import (
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hotelier/pkg/config"
)

// stubForge answers the users API with an avatar_url on its own origin and
// serves that one image.
func stubForge(t *testing.T) *httptest.Server {
	t.Helper()
	var srv *httptest.Server // assigned before any request can arrive
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"username":   strings.TrimPrefix(r.URL.Path, "/api/v1/users/"),
			"avatar_url": srv.URL + "/avatars/tile.png",
		})
	})
	mux.HandleFunc("/avatars/tile.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		img := image.NewRGBA(image.Rect(0, 0, 309, 309))
		img.Set(10, 10, color.RGBA{255, 0, 0, 255})
		_ = png.Encode(w, img)
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// Without forge_url the route exists but says so distinctly from "no such
// identity": a 503, never a body borrowed from anywhere.
func TestHandleAvatarDisabledWithoutForgeURL(t *testing.T) {
	srv := newTestServer(t)

	rec := httptest.NewRecorder()
	srv.HandleAvatar(rec, httptest.NewRequest(http.MethodGet, "/avatar/alice", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// With forge_url and an avatar cache dir configured, the server's route
// resolves a login through the proxy.
func TestHandleAvatarServesThroughConfiguredProxy(t *testing.T) {
	forge := stubForge(t)
	cfg := config.ServerConfig{
		Host:           "127.0.0.1",
		Port:           0,
		ForgeURL:       forge.URL,
		AvatarCacheDir: t.TempDir(),
	}
	s := New(cfg)

	rec := httptest.NewRecorder()
	s.HandleAvatar(rec, httptest.NewRequest(http.MethodGet, "/avatar/alice", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
		t.Errorf("Content-Type = %q, want image/png", ct)
	}
}

// A forge_url that cannot be parsed is logged at startup and leaves the
// route disabled rather than half-alive.
func TestHandleAvatarDisabledOnUnparsableForgeURL(t *testing.T) {
	cfg := config.ServerConfig{
		Host:           "127.0.0.1",
		Port:           0,
		ForgeURL:       "not a url",
		AvatarCacheDir: t.TempDir(),
	}
	s := New(cfg)

	rec := httptest.NewRecorder()
	s.HandleAvatar(rec, httptest.NewRequest(http.MethodGet, "/avatar/alice", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
