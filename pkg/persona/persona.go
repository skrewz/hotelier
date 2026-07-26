package persona

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// FileCopy represents a source-to-destination file copy mapping.
// Source is an absolute path on the guest machine.
// Destination is relative to the task's working directory.
type FileCopy struct {
	From string `yaml:"from" json:"from"` // absolute source path
	To   string `yaml:"to" json:"to"`     // relative destination path (within workdir)
}

// Persona defines a named set of environment variables and file copies
// that are applied to a task's working directory.
//
// Environment variable values can contain the placeholder <workpath>
// which is substituted with the actual task working directory at runtime.
type Persona struct {
	// Name is the unique identifier for this persona.
	Name string `yaml:"name" json:"name"`
	// Env is a map of environment variable names to values.
	// Values may contain <workpath> as a placeholder for the task's
	// working directory.
	Env map[string]string `yaml:"env" json:"env"`
	// Files is a list of file copy mappings. Files are copied from
	// their source paths into the task's working directory.
	Files []FileCopy `yaml:"files" json:"files"`
}

// ResolvedEnv returns the environment variables with <workpath> substituted
// by the given working directory path. Returns nil if the persona is nil.
func (p *Persona) ResolvedEnv(workDir string) map[string]string {
	if p == nil {
		return nil
	}
	resolved := make(map[string]string, len(p.Env))
	for k, v := range p.Env {
		resolved[k] = strings.ReplaceAll(v, "<workpath>", workDir)
	}
	return resolved
}

// ApplyFiles copies the persona's configured files into the working directory.
// It creates any necessary parent directories for the destination paths.
// If a source file does not exist, it is silently skipped (the file may
// not be relevant on this particular guest).
// Returns nil if the persona is nil.
func (p *Persona) ApplyFiles(workDir string) error {
	if p == nil {
		return nil
	}
	for _, fc := range p.Files {
		src := fc.From
		dst := filepath.Join(workDir, fc.To)

		// Skip if source does not exist — the file may not be present
		// on this particular guest machine.
		if _, err := os.Stat(src); err != nil {
			log.Printf("[PERSONA] skip %s (not found)", src)
			continue
		}

		// Log source file info for troubleshooting.
		srcInfo, err := os.Stat(src)
		if err != nil {
			return fmt.Errorf("stat source %s: %w", src, err)
		}
		log.Printf("[PERSONA] applying %s -> %s (src mode=%o, size=%d)", src, dst, srcInfo.Mode().Perm(), srcInfo.Size())

		// Create parent directory if needed.
		dstDir := filepath.Dir(dst)
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return fmt.Errorf("create directory %s: %w", dstDir, err)
		}

		// Read source file.
		data, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("read file %s: %w", src, err)
		}

		if err := os.WriteFile(dst, data, srcInfo.Mode().Perm()); err != nil {
			return fmt.Errorf("write file %s: %w", dst, err)
		}
	}
	return nil
}

// Store holds the configured personas for a hotelier instance.
type Store struct {
	personas map[string]*Persona
}

// NewStore creates a new persona store from the given personas.
func NewStore(personas []Persona) *Store {
	s := &Store{
		personas: make(map[string]*Persona, len(personas)),
	}
	for i := range personas {
		p := &personas[i] // safe: we're copying the pointer, not the struct
		s.personas[p.Name] = p
	}
	return s
}

// Get returns the persona with the given name, or an error if not found.
func (s *Store) Get(name string) (*Persona, error) {
	if name == "" {
		return nil, nil // empty persona name means no persona
	}
	p, ok := s.personas[name]
	if !ok {
		return nil, fmt.Errorf("persona %q not found", name)
	}
	// Return a copy to prevent mutation of the stored persona
	cp := *p
	cp.Env = make(map[string]string, len(p.Env))
	for k, v := range p.Env {
		cp.Env[k] = v
	}
	cp.Files = make([]FileCopy, len(p.Files))
	copy(cp.Files, p.Files)
	return &cp, nil
}

// Exists returns true if a persona with the given name exists.
func (s *Store) Exists(name string) bool {
	if name == "" {
		return true // empty persona is always valid (no persona)
	}
	_, ok := s.personas[name]
	return ok
}

// List returns the names of all configured personas.
func (s *Store) List() []string {
	names := make([]string, 0, len(s.personas))
	for name := range s.personas {
		names = append(names, name)
	}
	return names
}

// Validate checks that all personas have valid names and that file copy
// paths are well-formed. Returns an error for the first problem found.
func Validate(personas []Persona) error {
	seen := make(map[string]bool, len(personas))
	for _, p := range personas {
		if p.Name == "" {
			return fmt.Errorf("persona has empty name")
		}
		if seen[p.Name] {
			return fmt.Errorf("duplicate persona name: %q", p.Name)
		}
		seen[p.Name] = true

		for _, fc := range p.Files {
			if fc.From == "" {
				return fmt.Errorf("persona %q: file copy has empty source path", p.Name)
			}
			if fc.To == "" {
				return fmt.Errorf("persona %q: file copy has empty destination path", p.Name)
			}
			// Destination must not be absolute — it's relative to workdir
			if filepath.IsAbs(fc.To) {
				return fmt.Errorf("persona %q: file copy destination must be relative, got %q", p.Name, fc.To)
			}
			// Check for path traversal
			if strings.Contains(fc.To, "..") {
				return fmt.Errorf("persona %q: file copy destination must not contain .., got %q", p.Name, fc.To)
			}
		}
	}
	return nil
}

// ApplyPersona is a convenience function that applies a persona to a working
// directory: copies files and returns the resolved environment variables.
func ApplyPersona(p *Persona, workDir string) (map[string]string, error) {
	if p == nil {
		return nil, nil
	}

	if err := p.ApplyFiles(workDir); err != nil {
		return nil, fmt.Errorf("apply persona files: %w", err)
	}

	return p.ResolvedEnv(workDir), nil
}
