package server

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"testing"

	"hotelier/pkg/config"
)

// TestHostIsLoopbackName_IsTotal: the helper normalises for itself, so a
// caller that hands it a raw Host value—issue #285 is announced as the next
// consumer of this mechanism—gets the same verdict as one that folds, ports
// and brackets first. A precondition the signature cannot state must not be
// one a caller can silently get wrong into a 403.
func TestHostIsLoopbackName_IsTotal(t *testing.T) {
	for _, name := range []string{"localhost", "Localhost", "Localhost:8080", "127.0.0.1:8080", "[::1]:8080", "[::1]", "127.16.0.1", "LOCALHOST."} {
		if !hostIsLoopbackName(name) {
			t.Errorf("hostIsLoopbackName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"attacker.example", "hotelier.example.com", "hotelier.example.com.evil.example", "[::1", "hotelier.example.com]", "::1]", ""} {
		if hostIsLoopbackName(name) {
			t.Errorf("hostIsLoopbackName(%q) = true, want false", name)
		}
	}
}

// TestReload_ReportsTrustedHosts: every other hot-reloaded key says what
// landed, and this one matters most because its refusal says only that the
// caller is not the operator—an edit that lands silently and an edit that does
// not land produce the same evidence.
func TestReload_ReportsTrustedHosts(t *testing.T) {
	const maxTasks = 3
	base := func(hosts []string) config.ServerConfig {
		return config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: maxTasks, TrustedHosts: hosts}
	}
	srv := capTestServer(t, base(nil))

	var logBuf bytes.Buffer
	srv.log = log.New(&logBuf, "", 0)

	trustedHosts := []string{"hotelier.example.com", "[::1]"}
	srv.Reload(base(trustedHosts))
	if got := logBuf.String(); !strings.Contains(got, "trusted_hosts updated: 2 names") {
		t.Fatalf("expected the reload to report the names it took, got:\n%s", got)
	}
	if got := effectiveTrustedHosts(srv); !reflect.DeepEqual(got, trustedHosts) {
		t.Fatalf("expected the reload to install %q, got %q", trustedHosts, got)
	}

	// A reload that changes nothing says nothing, the way the other keys do.
	logBuf.Reset()
	srv.Reload(base(trustedHosts))
	if got := logBuf.String(); strings.Contains(got, "trusted_hosts") {
		t.Fatalf("expected no trusted_hosts line for an unchanged list, got:\n%s", got)
	}

	// An entry no Host header can ever carry is a configuration error: the
	// previous list stays in force and the operator hears why, the way an
	// unparseable schedule keeps the previous concurrency keys.
	logBuf.Reset()
	srv.Reload(base([]string{"*.example.com"}))
	got := logBuf.String()
	if !strings.Contains(got, "trusted_hosts") || !strings.Contains(got, "keeping previous trusted_hosts") {
		t.Fatalf("expected the reload to refuse the dead entry and say the old list stays, got:\n%s", got)
	}
	if hosts := effectiveTrustedHosts(srv); !reflect.DeepEqual(hosts, trustedHosts) {
		t.Fatalf("expected the refused reload to keep %q, got %q", trustedHosts, hosts)
	}
}

// effectiveTrustedHosts reads the list the guard is matching against.
func effectiveTrustedHosts(srv *Server) []string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.cfg.TrustedHosts
}
