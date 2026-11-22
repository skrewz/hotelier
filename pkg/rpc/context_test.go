package rpc

import (
	"context"
	"testing"
)

func TestContextWithConnection(t *testing.T) {
	ctx := ContextWithConnection(context.Background(), "conn-1")
	id, ok := ConnectionIDFromContext(ctx)
	if !ok || id != "conn-1" {
		t.Errorf("expected conn-1, got %q (ok=%v)", id, ok)
	}

	if _, ok := ConnectionIDFromContext(context.Background()); ok {
		t.Error("plain context must not carry a connection id")
	}

	if _, ok := ConnectionIDFromContext(nil); ok {
		t.Error("nil context must not carry a connection id")
	}
}
