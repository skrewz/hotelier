package guest

import (
	"strings"
	"testing"

	"hotelier/pkg/config"
)

// Issue #253: README documents `id` in the guest configuration, but New always
// minted a random one, so a process restart renamed the guest and orphaned
// whatever the registry had assigned to the old identity.

func TestNew_HonoursConfiguredID(t *testing.T) {
	g := New(config.GuestConfig{ID: "guest-producer-1", Name: "Producer"}, nil)

	if got := g.ID(); got != "guest-producer-1" {
		t.Errorf("expected the configured id to be used, got %q", got)
	}
}

func TestNew_GeneratesIDWhenUnset(t *testing.T) {
	g := New(config.GuestConfig{Name: "Anonymous"}, nil)

	id := g.ID()
	if !strings.HasPrefix(id, "guest-") {
		t.Fatalf("expected a generated id with the guest- prefix, got %q", id)
	}
	if id == "guest-" {
		t.Fatalf("generated id %q carries no entropy", id)
	}

	// Two guests configured without an id must not collide.
	other := New(config.GuestConfig{Name: "Anonymous"}, nil)
	if other.ID() == id {
		t.Errorf("two guests without a configured id share %q", id)
	}
}

func TestNew_LogPrefixUsesConfiguredID(t *testing.T) {
	g := New(config.GuestConfig{ID: "guest-producer-1"}, nil)

	if !strings.Contains(g.log.Prefix(), "guest-producer-1") {
		t.Errorf("log prefix %q does not name the configured id", g.log.Prefix())
	}
}

// The register payload has to say whether this process has registered before:
// with a stable id, "id already in the registry" no longer implies "the same
// process is back", and the server must not reconcile a fresh process against
// a task it has never been given.
func TestRegisterParams_ResumedFlag(t *testing.T) {
	g := New(config.GuestConfig{ID: "guest-producer-1", Name: "Producer", Tags: []string{"tag1"}}, nil)

	first := g.registerParams()
	if resumed, ok := first["resumed"].(bool); !ok || resumed {
		t.Errorf("first registration should say resumed=false, got %v", first["resumed"])
	}
	if first["id"] != "guest-producer-1" {
		t.Errorf("expected id guest-producer-1, got %v", first["id"])
	}

	// A reconnection from the same process is a reconnect.
	g.markRegistered()

	second := g.registerParams()
	if resumed, ok := second["resumed"].(bool); !ok || !resumed {
		t.Errorf("re-registration should say resumed=true, got %v", second["resumed"])
	}
}
