package persona

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersona_ResolvedEnv(t *testing.T) {
	p := &Persona{
		Name: "test-persona",
		Env: map[string]string{
			"FORGEJO_TOKEN_FILE": "<workpath>/.tokens/test",
			"STATIC_VAR":         "constant-value",
			"MULTI_PLACEHOLDER":  "<workpath>/data/<workpath>/logs",
		},
	}

	resolved := p.ResolvedEnv("/tmp/hotelier/tasks/task-123")

	if resolved["FORGEJO_TOKEN_FILE"] != "/tmp/hotelier/tasks/task-123/.tokens/test" {
		t.Errorf("unexpected FORGEJO_TOKEN_FILE: %q", resolved["FORGEJO_TOKEN_FILE"])
	}
	if resolved["STATIC_VAR"] != "constant-value" {
		t.Errorf("unexpected STATIC_VAR: %q", resolved["STATIC_VAR"])
	}
	if resolved["MULTI_PLACEHOLDER"] != "/tmp/hotelier/tasks/task-123/data//tmp/hotelier/tasks/task-123/logs" {
		t.Errorf("unexpected MULTI_PLACEHOLDER: %q", resolved["MULTI_PLACEHOLDER"])
	}
}

func TestPersona_ResolvedEnv_nil_safe(t *testing.T) {
	var p *Persona
	resolved := p.ResolvedEnv("/tmp/test")
	if resolved != nil {
		t.Errorf("expected nil for nil persona, got: %v", resolved)
	}
}

func TestPersona_ApplyFiles(t *testing.T) {
	// Create a temp directory for the working directory
	workDir := t.TempDir()

	// Create a source file
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "secret-token")
	if err := os.WriteFile(srcFile, []byte("my-secret-token"), 0o600); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	p := &Persona{
		Name: "test-persona",
		Files: []FileCopy{
			{From: srcFile, To: ".tokens/secret-token"},
		},
	}

	if err := p.ApplyFiles(workDir); err != nil {
		t.Fatalf("ApplyFiles failed: %v", err)
	}

	// Check that the file was copied
	dstFile := filepath.Join(workDir, ".tokens", "secret-token")
	data, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read copied file: %v", err)
	}
	if string(data) != "my-secret-token" {
		t.Errorf("unexpected content: %q", string(data))
	}

	// Check that permissions were preserved
	info, err := os.Stat(dstFile)
	if err != nil {
		t.Fatalf("failed to stat copied file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("unexpected permissions: %o", info.Mode().Perm())
	}
}

func TestPersona_ApplyFiles_missing_source_skipped(t *testing.T) {
	workDir := t.TempDir()

	p := &Persona{
		Name: "test-persona",
		Files: []FileCopy{
			{From: "/nonexistent/path/to/file", To: ".tokens/missing"},
		},
	}

	// Should not error — missing source files are silently skipped
	if err := p.ApplyFiles(workDir); err != nil {
		t.Fatalf("ApplyFiles should skip missing source files: %v", err)
	}

	// File should not exist
	dstFile := filepath.Join(workDir, ".tokens", "missing")
	if _, err := os.Stat(dstFile); !os.IsNotExist(err) {
		t.Errorf("expected file to not exist, but it does")
	}
}

func TestPersona_ApplyFiles_multiple_files(t *testing.T) {
	workDir := t.TempDir()
	srcDir := t.TempDir()

	// Create multiple source files
	files := map[string]string{
		"token1":  "secret1",
		"token2":  "secret2",
		"config1": "git config content",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(srcDir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("failed to create source file %s: %v", name, err)
		}
	}

	p := &Persona{
		Name: "multi-files",
		Files: []FileCopy{
			{From: filepath.Join(srcDir, "token1"), To: ".tokens/s-autonomics-implementer"},
			{From: filepath.Join(srcDir, "token2"), To: ".tokens/s-autonomics-reviewer"},
			{From: filepath.Join(srcDir, "config1"), To: ".forgejo-gitconfigs/s-autonomics-implementer"},
		},
	}

	if err := p.ApplyFiles(workDir); err != nil {
		t.Fatalf("ApplyFiles failed: %v", err)
	}

	// Verify all files were copied
	expectedFiles := map[string]string{
		".tokens/s-autonomics-implementer":             "secret1",
		".tokens/s-autonomics-reviewer":                "secret2",
		".forgejo-gitconfigs/s-autonomics-implementer": "git config content",
	}
	for relPath, expectedContent := range expectedFiles {
		fullPath := filepath.Join(workDir, relPath)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("failed to read %s: %v", relPath, err)
			continue
		}
		if string(data) != expectedContent {
			t.Errorf("unexpected content for %s: got %q, want %q", relPath, string(data), expectedContent)
		}
	}
}

func TestPersona_ApplyFiles_readonly_source_reapply(t *testing.T) {
	// Regression test: when a source file has mode 0o400 (read-only),
	// re-applying it after it already exists in the work dir must not
	// fail with "permission denied". This can happen when ApplyFiles is
	// called twice — e.g. before and after a repo clone.
	workDir := t.TempDir()
	srcDir := t.TempDir()

	srcFile := filepath.Join(srcDir, "token")
	if err := os.WriteFile(srcFile, []byte("secret"), 0o400); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	p := &Persona{
		Name: "test-persona",
		Files: []FileCopy{
			{From: srcFile, To: ".tokens/token"},
		},
	}

	// First apply — creates the file with mode 0o400.
	if err := p.ApplyFiles(workDir); err != nil {
		t.Fatalf("first ApplyFiles failed: %v", err)
	}

	// Verify first apply set read-only permissions.
	info, err := os.Stat(filepath.Join(workDir, ".tokens", "token"))
	if err != nil {
		t.Fatalf("failed to stat dest file: %v", err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Errorf("expected mode 0o400 after first apply, got %o", info.Mode().Perm())
	}

	// Second apply — must not fail even though the dest is read-only.
	if err := p.ApplyFiles(workDir); err != nil {
		t.Fatalf("second ApplyFiles failed (re-apply of read-only source): %v", err)
	}

	// Verify the file was overwritten with correct content.
	data, err := os.ReadFile(filepath.Join(workDir, ".tokens", "token"))
	if err != nil {
		t.Fatalf("failed to read overwritten file: %v", err)
	}
	if string(data) != "secret" {
		t.Errorf("unexpected content: %q", string(data))
	}
}

func TestStore_Get(t *testing.T) {
	store := NewStore([]Persona{
		{Name: "alpha", Env: map[string]string{"A": "1"}},
		{Name: "beta", Env: map[string]string{"B": "2"}},
	})

	p, err := store.Get("alpha")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Name != "alpha" {
		t.Errorf("unexpected name: %q", p.Name)
	}
	if p.Env["A"] != "1" {
		t.Errorf("unexpected env: %v", p.Env)
	}

	_, err = store.Get("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent persona")
	}
}

func TestStore_Get_empty_name_returns_nil(t *testing.T) {
	store := NewStore([]Persona{
		{Name: "alpha", Env: map[string]string{"A": "1"}},
	})

	p, err := store.Get("")
	if err != nil {
		t.Fatalf("unexpected error for empty name: %v", err)
	}
	if p != nil {
		t.Errorf("expected nil persona for empty name, got: %+v", p)
	}
}

func TestStore_Get_returns_copy(t *testing.T) {
	store := NewStore([]Persona{
		{Name: "alpha", Env: map[string]string{"A": "1"}},
	})

	p1, _ := store.Get("alpha")
	p1.Env["A"] = "modified"

	p2, _ := store.Get("alpha")
	if p2.Env["A"] != "1" {
		t.Errorf("store was mutated by Get: got %q, want %q", p2.Env["A"], "1")
	}
}

func TestStore_Exists(t *testing.T) {
	store := NewStore([]Persona{
		{Name: "alpha"},
	})

	if !store.Exists("alpha") {
		t.Error("expected alpha to exist")
	}
	if store.Exists("beta") {
		t.Error("expected beta to not exist")
	}
	if !store.Exists("") {
		t.Error("empty persona name should always exist (no persona)")
	}
}

func TestStore_List(t *testing.T) {
	store := NewStore([]Persona{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	})

	names := store.List()
	if len(names) != 3 {
		t.Fatalf("expected 3 personas, got %d", len(names))
	}

	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	for _, expected := range []string{"alpha", "beta", "gamma"} {
		if !nameSet[expected] {
			t.Errorf("missing persona name: %q", expected)
		}
	}
}

func TestValidate_valid_personas(t *testing.T) {
	personas := []Persona{
		{
			Name: "test",
			Env:  map[string]string{"VAR": "value"},
			Files: []FileCopy{
				{From: "/absolute/path/to/file", To: ".tokens/file"},
			},
		},
	}

	if err := Validate(personas); err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}
}

func TestValidate_empty_name(t *testing.T) {
	err := Validate([]Persona{{Name: ""}})
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestValidate_duplicate_name(t *testing.T) {
	err := Validate([]Persona{
		{Name: "dup"},
		{Name: "dup"},
	})
	if err == nil {
		t.Error("expected error for duplicate name")
	}
}

func TestValidate_empty_source_path(t *testing.T) {
	err := Validate([]Persona{
		{Name: "test", Files: []FileCopy{{From: "", To: "dest"}}},
	})
	if err == nil {
		t.Error("expected error for empty source path")
	}
}

func TestValidate_empty_dest_path(t *testing.T) {
	err := Validate([]Persona{
		{Name: "test", Files: []FileCopy{{From: "/src", To: ""}}},
	})
	if err == nil {
		t.Error("expected error for empty dest path")
	}
}

func TestValidate_absolute_dest_path(t *testing.T) {
	err := Validate([]Persona{
		{Name: "test", Files: []FileCopy{{From: "/src", To: "/absolute/dest"}}},
	})
	if err == nil {
		t.Error("expected error for absolute dest path")
	}
}

func TestValidate_path_traversal(t *testing.T) {
	err := Validate([]Persona{
		{Name: "test", Files: []FileCopy{{From: "/src", To: "../../../etc/passwd"}}},
	})
	if err == nil {
		t.Error("expected error for path traversal")
	}
}

func TestApplyPersona_nil_persona(t *testing.T) {
	env, err := ApplyPersona(nil, "/tmp/test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env != nil {
		t.Errorf("expected nil env for nil persona, got: %v", env)
	}
}

func TestApplyPersona_full_flow(t *testing.T) {
	workDir := t.TempDir()
	srcDir := t.TempDir()

	// Create a source file
	srcFile := filepath.Join(srcDir, "token")
	if err := os.WriteFile(srcFile, []byte("my-token"), 0o600); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	p := &Persona{
		Name: "full-test",
		Env: map[string]string{
			"TOKEN_FILE": "<workpath>/.tokens/token",
		},
		Files: []FileCopy{
			{From: srcFile, To: ".tokens/token"},
		},
	}

	env, err := ApplyPersona(p, workDir)
	if err != nil {
		t.Fatalf("ApplyPersona failed: %v", err)
	}

	// Check env vars
	if env["TOKEN_FILE"] != filepath.Join(workDir, ".tokens/token") {
		t.Errorf("unexpected TOKEN_FILE: %q", env["TOKEN_FILE"])
	}

	// Check file was copied
	dstFile := filepath.Join(workDir, ".tokens", "token")
	data, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read copied file: %v", err)
	}
	if string(data) != "my-token" {
		t.Errorf("unexpected content: %q", string(data))
	}
}
