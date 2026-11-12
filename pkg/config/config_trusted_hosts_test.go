package config

import (
	"strings"
	"testing"
)

// TestNormaliseHostName pins what "one name" means for both halves of the
// trusted_hosts comparison (issue #286): port stripped, a balanced pair of
// IPv6 brackets stripped, one trailing dot stripped, case folded. Unbalanced
// brackets are left alone, because a Host value carrying them is not a name
// any client speaks and the normaliser must not invent one for it.
func TestNormaliseHostName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"hotelier.example.com", "hotelier.example.com"},
		{"Hotelier.Example.Com:8443", "hotelier.example.com"},
		{"hotelier.example.com.", "hotelier.example.com"},
		{"Hotelier.Example.Com:8443.", "hotelier.example.com"},
		{"localhost:8080", "localhost"},
		{"[::1]:8080", "::1"},
		{"[::1]", "::1"},
		{"[hotelier.example.com]", "hotelier.example.com"},
		{"[::1", "[::1"},
		{"hotelier.example.com]", "hotelier.example.com]"},
		{"127.0.0.1", "127.0.0.1"},
		{"", ""},
		{" hotelier.example.com", " hotelier.example.com"},
	}
	for _, tc := range tests {
		if got := NormaliseHostName(tc.in); got != tc.want {
			t.Errorf("NormaliseHostName(%q) = %q, want %q", tc.in, got, tc.want)
		}
		// The guard calls the normaliser on a value a caller may already have
		// normalised, so running it twice must be a no-op.
		if twice := NormaliseHostName(NormaliseHostName(tc.in)); twice != tc.want {
			t.Errorf("NormaliseHostName is not idempotent for %q: %q then %q", tc.in, tc.want, twice)
		}
	}
}

// TestValidateTrustedHosts: an entry whose normalised form no Host header can
// ever carry is a configuration error, not a name that quietly matches nothing
// for the life of the process—the same 403 the field exists to fix, but
// looking like a broken guard rather than a typo.
func TestValidateTrustedHosts(t *testing.T) {
	ok := [][]string{
		nil,
		{},
		{"hotelier.example.com"},
		{"Hotelier.Example.Com:8443."},
		{"hotelier", "localhost"},
		// The normaliser drops whatever follows the last colon, so a port that
		// is not a number still leaves a name the route will admit.
		{"hotelier.example.com:abcd"},
		{"127.0.0.1", "::1", "[::1]", "hotelier.example.local", "hotel_host", "a-b.example.com"},
	}
	for _, hosts := range ok {
		if err := (ServerConfig{TrustedHosts: hosts}).ValidateTrustedHosts(); err != nil {
			t.Errorf("trusted_hosts %q: expected no error, got %v", hosts, err)
		}
	}

	dead := []struct {
		entry   string
		wantErr string
	}{
		{"", "empty"},
		{"   ", ""},
		{"*.example.com", "wildcard"},
		{".example.com", ""},
		{"https://hotelier.example.com", ""},
		{"http://hotelier.example.com", ""},
		{"hotelier..example", ""},
		{"hotelier.example.com/path", ""},
		{" hotelier.example.com", ""},
		{"hotelier.example.com ", ""},
		{"hotelier-.example.com", ""},
	}
	for _, tc := range dead {
		err := (ServerConfig{TrustedHosts: []string{"hotelier.example.com", tc.entry}}).ValidateTrustedHosts()
		if err == nil {
			t.Errorf("trusted_hosts entry %q: expected it refused as unable to ever match", tc.entry)
			continue
		}
		if !strings.Contains(err.Error(), "trusted_hosts") {
			t.Errorf("trusted_hosts entry %q: expected the error to name the key, got %v", tc.entry, err)
		}
		if !strings.Contains(err.Error(), tc.entry) && tc.entry != "   " {
			t.Errorf("trusted_hosts entry %q: expected the error to quote the entry as written, got %v", tc.entry, err)
		}
		if tc.wantErr != "" && !strings.Contains(strings.ToLower(err.Error()), tc.wantErr) {
			t.Errorf("trusted_hosts entry %q: expected the error to mention %q, got %v", tc.entry, tc.wantErr, err)
		}
	}
}

// TestLoadServerConfig_TrustedHosts: the file path refuses a dead entry the
// way it refuses an unparseable schedule, so the operator hears about it at
// startup instead of at the first 403.
func TestLoadServerConfig_TrustedHosts(t *testing.T) {
	path := writeServerConfig(t, "trusted_hosts:\n  - \"Hotelier.Example.Com\"\n  - \"[::1]\"\n")
	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("LoadServerConfig failed: %v", err)
	}
	if len(cfg.TrustedHosts) != 2 {
		t.Fatalf("expected 2 trusted_hosts entries, got %q", cfg.TrustedHosts)
	}

	path = writeServerConfig(t, "trusted_hosts:\n  - \"*.example.com\"\n")
	if _, err := LoadServerConfig(path); err == nil {
		t.Fatal("expected a wildcard trusted_hosts entry to be refused at load")
	} else if !strings.Contains(err.Error(), "trusted_hosts") {
		t.Errorf("expected the load error to name trusted_hosts, got %v", err)
	}
}
