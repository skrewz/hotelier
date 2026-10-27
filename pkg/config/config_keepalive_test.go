package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDefaultKeepaliveConfig pins the WebSocket keepalive defaults
// (issue #220): the ping interval must be noticeably shorter than the
// default heartbeat interval (30s) and the pong timeout must stay well
// inside any plausible proxy idle timeout (nginx default: 60s).
func TestDefaultKeepaliveConfig(t *testing.T) {
	g := DefaultGuestConfig()
	if g.KeepalivePingInterval != 10 {
		t.Errorf("guest keepalive_ping_interval default = %d, want 10", g.KeepalivePingInterval)
	}
	if g.KeepalivePongTimeout != 30 {
		t.Errorf("guest keepalive_pong_timeout default = %d, want 30", g.KeepalivePongTimeout)
	}

	s := DefaultServerConfig()
	if s.KeepalivePingInterval != 10 {
		t.Errorf("server keepalive_ping_interval default = %d, want 10", s.KeepalivePingInterval)
	}
	if s.KeepalivePongTimeout != 30 {
		t.Errorf("server keepalive_pong_timeout default = %d, want 30", s.KeepalivePongTimeout)
	}
}

// TestLoadGuestConfigKeepalive verifies the keepalive knobs can be set
// from the guest YAML so a deployment can tune them without a code change.
func TestLoadGuestConfigKeepalive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "guest.yaml")
	body := "url: \"ws://hotelier.example.com:8080/ws\"\n" +
		"id: guest-1\n" +
		"keepalive_ping_interval: 5\n" +
		"keepalive_pong_timeout: 12\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadGuestConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KeepalivePingInterval != 5 {
		t.Errorf("keepalive_ping_interval = %d, want 5", cfg.KeepalivePingInterval)
	}
	if cfg.KeepalivePongTimeout != 12 {
		t.Errorf("keepalive_pong_timeout = %d, want 12", cfg.KeepalivePongTimeout)
	}
}
