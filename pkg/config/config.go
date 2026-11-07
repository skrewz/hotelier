package config

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"hotelier/pkg/persona"

	"gopkg.in/yaml.v3"
)

// ServerConfig holds the configuration for the Check-In Host server.
type ServerConfig struct {
	// Host is the address to bind the server to.
	Host string `yaml:"host"`
	// Port is the port to listen on.
	Port int `yaml:"port"`
	// ReadTimeout is the maximum duration for reading the entire request.
	ReadTimeout int `yaml:"read_timeout"`
	// WriteTimeout is the maximum duration before timing out writes.
	WriteTimeout int `yaml:"write_timeout"`
	// MaxLogSize is the maximum size of a log entry in bytes (default 1MB).
	MaxLogSize int `yaml:"max_log_size"`
	// TaskTimeout is the default timeout for tasks in seconds (0 = unlimited).
	TaskTimeout int `yaml:"task_timeout"`
	// HeartbeatInterval is how often guests must send heartbeats (seconds).
	HeartbeatInterval int `yaml:"heartbeat_interval"`
	// SilenceTimeout is the duration a guest may be silent (no heartbeats)
	// before the server considers it stale: any running task is failed and the
	// guest is removed from the registry. Set to 0 to disable stale-guest
	// removal. Default: 180 seconds (6 missed heartbeats at the default 30s
	// interval).
	SilenceTimeout int `yaml:"silence_timeout"`
	// TaskAssignmentTimeout is the duration before an ASSIGNED task is considered
	// stuck. If a task has been ASSIGNED for longer than this and the guest has
	// not heartbeated with that task_id, the server fails the task and re-queues
	// it. Default: HeartbeatInterval * 3 (90 seconds).
	TaskAssignmentTimeout int `yaml:"task_assignment_timeout"`
	// TaskSilenceTimeout is the duration a RUNNING guest can be silent before
	// its task is failed. This is separate from SilenceTimeout (which governs
	// when a guest connection is considered stale). Default: 600 seconds (10 minutes).
	TaskSilenceTimeout int `yaml:"task_silence_timeout"`
	// MaxGuests is the maximum number of guests allowed (0 = unlimited).
	MaxGuests int `yaml:"max_guests"`
	// MaxConcurrentTasks is the static cap on how many tasks the server is
	// willing to run at any moment (0 = unlimited). Used when
	// ConcurrencySchedule is empty; the schedule takes precedence otherwise
	// (issue #248).
	MaxConcurrentTasks int `yaml:"max_concurrent_tasks"`
	// ConcurrencySchedule is a list of daily wall-clock boundaries for the
	// concurrency cap. Each entry takes effect at its At time (interpreted
	// in ScheduleTimezone) and holds until the next entry; the last entry
	// of the day holds through midnight to the first (issue #248).
	ConcurrencySchedule []ConcurrencyScheduleEntry `yaml:"concurrency_schedule"`
	// ScheduleTimezone is the IANA zone name ConcurrencySchedule At times
	// are interpreted in (e.g. "Australia/Sydney"), so boundaries follow
	// the wall clock across DST. Default: "UTC".
	ScheduleTimezone string `yaml:"schedule_timezone"`
	// LogDir is the base directory where task logs are persisted to disk.
	// When empty, logs are kept in memory only.
	LogDir string `yaml:"log_dir"`
	// QueueDir is the directory where unprocessed tasks (pending, assigned,
	// running) are persisted as JSON files, one per task, in per-status
	// subdirectories. When the server starts, persisted tasks are restored
	// to the queue as PENDING (issue #190). When empty, the queue is
	// in-memory only.
	QueueDir string `yaml:"queue_dir"`
	// Personas is a list of persona definitions. Each persona specifies
	// environment variables and file copies that are applied to a task's
	// working directory. See pkg/persona for details.
	Personas []persona.Persona `yaml:"personas"`
}

// ConcurrencyScheduleEntry is one boundary of the daily concurrency
// schedule (issue #248).
type ConcurrencyScheduleEntry struct {
	// At is the wall-clock time the entry takes effect, in strict
	// 24-hour "HH:MM" form, interpreted in the config's schedule_timezone.
	At string `yaml:"at"`
	// Concurrency is the cap in force from At until the next entry
	// (0 pauses all work).
	Concurrency int `yaml:"concurrency"`
}

// GuestConfig holds the configuration for a guest.
type GuestConfig struct {
	// URL is the full WebSocket URL of the Check-In Host (e.g. "wss://host:443/ws").
	URL string `yaml:"url"`
	// ID is the unique identifier for this guest.
	ID string `yaml:"id"`
	// Name is a human-readable name for this guest.
	Name string `yaml:"name"`
	// Tags are the capabilities this guest declares.
	Tags []string `yaml:"tags"`
	// TaskTimeout is the timeout for tasks in seconds (0 = use server default).
	TaskTimeout int `yaml:"task_timeout"`
	// HeartbeatInterval is how often to send heartbeats (seconds, 0 = use server default).
	HeartbeatInterval int `yaml:"heartbeat_interval"`
	// SilenceTimeout is the duration of RPC silence before the server kills the task.
	// 0 = use server default.
	SilenceTimeout int `yaml:"silence_timeout"`
	// WorkingDir is the base working directory for task execution.
	WorkingDir string `yaml:"working_dir"`
	// LogLevel is the logging level (debug, info, warn, error).
	LogLevel string `yaml:"log_level"`
	// ClientCert is the path to the TLS client certificate for mTLS
	// authentication with the Check-In Host. When set, the guest will
	// present this certificate during the TLS handshake.
	ClientCert string `yaml:"client_cert"`
	// ClientKey is the path to the TLS client private key for mTLS
	// authentication with the Check-In Host. Must be set together
	// with ClientCert.
	ClientKey string `yaml:"client_key"`
	// HomeCopies is the set of home paths copied into every task's
	// namespace jail (issue #201). Entries are relative to the guest's
	// home directory: directories are copied recursively (symlinks
	// resolved), files are copied with their content and mode. Entries
	// that do not exist on this guest are skipped. When the field is
	// absent or empty the jail falls back to its built-in default set.
	HomeCopies []string `yaml:"home_copies"`
}

// DefaultServerConfig returns a ServerConfig with sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Host:                  "0.0.0.0",
		Port:                  8080,
		ReadTimeout:           30,
		WriteTimeout:          30,
		MaxLogSize:            1024 * 1024, // 1MB
		TaskTimeout:           3600,        // 1 hour
		HeartbeatInterval:     30,
		SilenceTimeout:        180, // 6 heartbeats missed (6 * 30s)
		TaskAssignmentTimeout: 90,  // 3 heartbeats missed (3 * 30s)
		TaskSilenceTimeout:    600, // 10 minutes of silence while running
		MaxGuests:             0,   // unlimited
		MaxConcurrentTasks:    0,   // unlimited
		ScheduleTimezone:      "UTC",
	}
}

// DefaultGuestConfig returns a GuestConfig with sensible defaults.
func DefaultGuestConfig() GuestConfig {
	return GuestConfig{
		TaskTimeout:       0, // use server default
		HeartbeatInterval: 0, // use server default
		LogLevel:          "info",
		ClientCert:        "",
		ClientKey:         "",
	}
}

// ConnectURL returns the WebSocket URL to connect to. The scheme defaults to
// "ws" unless mTLS is configured (then "wss").
func (g *GuestConfig) ConnectURL(hasMTLS bool) (string, error) {
	raw := g.URL
	if raw == "" {
		return "", fmt.Errorf("url is required")
	}

	// url.Parse requires a scheme to distinguish host from path.
	// If no scheme is present, prepend http:// as a temporary placeholder
	// so the host/path are parsed correctly, then replace the scheme.
	if !strings.Contains(raw, "://") {
		if hasMTLS {
			raw = "wss://" + raw
		} else {
			raw = "ws://" + raw
		}
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("url must have a host")
	}
	return u.String(), nil
}

// LoadServerConfig reads and parses a server config file.
func LoadServerConfig(path string) (ServerConfig, error) {
	cfg := DefaultServerConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if err := cfg.ValidateConcurrency(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// atRe matches a strict 24-hour "HH:MM" wall-clock time (zero-padded).
var atRe = regexp.MustCompile(`^([01][0-9]|2[0-3]):([0-5][0-9])$`)

// ValidateConcurrency checks the concurrency schedule keys (issue #248):
// an unparseable zone, a malformed or duplicate "at", or a negative
// concurrency is a configuration error, never silently ignored.
//
// LoadServerConfig calls it, and so does every path that installs a config
// into a running server (Server.New / Server.Reload), because a config
// assembled in code never passes through the loader.
func (c ServerConfig) ValidateConcurrency() error {
	if c.MaxConcurrentTasks < 0 {
		return fmt.Errorf("max_concurrent_tasks must not be negative (got %d)", c.MaxConcurrentTasks)
	}
	if c.ScheduleTimezone != "" {
		if _, err := loadLocation(c.ScheduleTimezone); err != nil {
			return fmt.Errorf("schedule_timezone %q is not a valid IANA zone: %w", c.ScheduleTimezone, err)
		}
	}
	seen := make(map[int]string, len(c.ConcurrencySchedule))
	for _, e := range c.ConcurrencySchedule {
		minutes, err := parseScheduleAt(e.At)
		if err != nil {
			return fmt.Errorf("concurrency_schedule: %w", err)
		}
		if prev, dup := seen[minutes]; dup {
			return fmt.Errorf("concurrency_schedule: duplicate at %q", prev)
		}
		seen[minutes] = e.At
		if e.Concurrency < 0 {
			return fmt.Errorf("concurrency_schedule entry at %q: concurrency must not be negative (got %d)", e.At, e.Concurrency)
		}
	}
	return nil
}

// parseScheduleAt parses a strict "HH:MM" time into minutes past midnight.
func parseScheduleAt(at string) (int, error) {
	m := atRe.FindStringSubmatch(at)
	if m == nil {
		return 0, fmt.Errorf("invalid at %q: expected a 24-hour wall-clock time in \"HH:MM\" form", at)
	}
	h, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, fmt.Errorf("invalid at %q: %w", at, err)
	}
	mm, err := strconv.Atoi(m[2])
	if err != nil {
		return 0, fmt.Errorf("invalid at %q: %w", at, err)
	}
	return h*60 + mm, nil
}

// EffectiveConcurrency returns the concurrency cap in force at now: the
// last schedule entry whose "at" is at or before now in schedule_timezone,
// wrapping to the previous day's last entry before the first one; or
// MaxConcurrentTasks when no schedule is configured. Entries need not be
// sorted.
//
// A schedule this function cannot trust (an unparsable "at", an unloadable
// zone) resolves to MaxConcurrentTasks rather than to a guess. ValidateConcurrency
// rejects such a config at load and reload time, so reaching those branches
// means the config was built in code without ever being validated.
func (c ServerConfig) EffectiveConcurrency(now time.Time) int {
	if len(c.ConcurrencySchedule) == 0 {
		return c.MaxConcurrentTasks
	}

	type entry struct {
		minutes int
		value   int
	}
	entries := make([]entry, 0, len(c.ConcurrencySchedule))
	for _, e := range c.ConcurrencySchedule {
		m, err := parseScheduleAt(e.At)
		if err != nil {
			// Dropping the entry would silently hand its window to a
			// neighbouring cap, so fall back to the static cap instead.
			return c.MaxConcurrentTasks
		}
		entries = append(entries, entry{minutes: m, value: e.Concurrency})
	}
	if len(entries) == 0 {
		return c.MaxConcurrentTasks
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].minutes < entries[j].minutes })

	loc, err := loadLocation(c.ScheduleTimezone)
	if err != nil {
		// Evaluating the schedule in UTC would shift every boundary by the
		// zone's offset; the static cap is the honest answer.
		return c.MaxConcurrentTasks
	}
	local := now.In(loc)
	cur := local.Hour()*60 + local.Minute()

	// Default: the last entry of the day holds through midnight to the
	// first entry. Walk forward to the last boundary at or before now.
	chosen := entries[len(entries)-1]
	for _, e := range entries {
		if e.minutes <= cur {
			chosen = e
		}
	}
	return chosen.value
}

// zoneLocations memoises resolved *time.Location values by zone name.
// time.LoadLocation has no cache of its own and re-reads the zone file on
// every call; the enforcement member calls EffectiveConcurrency on every
// assignment decision, so without this the resolver would do disk I/O per
// decision.
var zoneLocations sync.Map

// loadLocation returns the named zone, caching successful lookups. It keeps
// the semantics of time.LoadLocation ("" and "Local" included).
func loadLocation(name string) (*time.Location, error) {
	if loc, ok := zoneLocations.Load(name); ok {
		return loc.(*time.Location), nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	zoneLocations.Store(name, loc)
	return loc, nil
}

// LoadGuestConfig reads and parses a guest config file.
func LoadGuestConfig(path string) (GuestConfig, error) {
	cfg := DefaultGuestConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if err := validateHomeCopies(cfg.HomeCopies); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// validateHomeCopies checks the jail home copy set (issue #201). Entries
// are resolved against the guest's home directory when a task's jail is
// built, so they must be non-empty relative paths: absolute paths and
// ".." components are rejected.
func validateHomeCopies(entries []string) error {
	for _, e := range entries {
		if e == "" {
			return fmt.Errorf("home_copies contains an empty entry; entries must be non-empty paths relative to the home directory")
		}
		if filepath.IsAbs(e) {
			return fmt.Errorf("home_copies entry %q must be relative to the home directory", e)
		}
		for _, part := range strings.Split(e, "/") {
			if part == ".." {
				return fmt.Errorf("home_copies entry %q must not escape the home directory (no \"..\" components)", e)
			}
		}
	}
	return nil
}

// TLSConfig builds a *tls.Config for mTLS client authentication.
// Returns nil if neither ClientCert nor ClientKey is set.
// Returns an error if the cert or key files cannot be read.
func (g *GuestConfig) TLSConfig() (*tls.Config, error) {
	if g.ClientCert == "" && g.ClientKey == "" {
		return nil, nil
	}

	if g.ClientCert == "" || g.ClientKey == "" {
		return nil, os.ErrInvalid
	}

	cert, err := tls.LoadX509KeyPair(g.ClientCert, g.ClientKey)
	if err != nil {
		return nil, fmt.Errorf("load client cert/key: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      nil, // trust system CAs
	}, nil
}

// ConfigStore provides a thread-safe way to store and retrieve configurations.
type ConfigStore struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// NewConfigStore creates a new ConfigStore.
func NewConfigStore() *ConfigStore {
	return &ConfigStore{
		data: make(map[string]interface{}),
	}
}

// Set stores a configuration value.
func (s *ConfigStore) Set(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a configuration value.
func (s *ConfigStore) Get(key string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}
