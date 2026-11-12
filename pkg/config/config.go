package config

import (
	"crypto/tls"
	"fmt"
	"net"
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
	// TrustedHosts names the host names a loopback peer may address this
	// server by in place of a loopback one (issue #286). Behind a local proxy
	// that forwards the incoming Host it is how the live cap stays reachable
	// from the dashboard; unset leaves loopback-only verdicts, exactly as
	// before. The rule itself, the normalisation it matches on, why there are
	// no wildcards and why an unmatchable entry is refused here all belong to
	// the guard that enforces them—see Server.operatorAllowed and
	// ValidateTrustedHosts. That the proxy must then refuse this route to
	// everyone but the operator is the README's to say.
	TrustedHosts []string `yaml:"trusted_hosts"`
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
	// (issue #248). EffectiveConcurrency reports the uncapped case as
	// ConcurrencyUnlimited rather than as 0, because a schedule entry uses 0
	// for the opposite: a full pause.
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
	// LogRetentionDays is how many days of task logs the prune sweep keeps:
	// date directories older than today minus this many days are removed at
	// startup and on the cleanup tick (issue #263). Following the max_guests
	// convention, 0 disables pruning entirely. Default: 14.
	LogRetentionDays int `yaml:"log_retention_days"`
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

// ConcurrencyUnlimited is the value EffectiveConcurrency returns when no cap
// is in force, so that "unlimited" cannot be mistaken for the 0 a schedule
// entry uses to pause all work (max_concurrent_tasks itself keeps the
// max_guests convention, where 0 means unlimited, and the resolver translates
// it into this sentinel).
//
// It is deliberately negative: ValidateConcurrency rejects a negative cap in
// either key, so no configuration can carry it and a resolver result is never
// ambiguous. A consumer must compare against ConcurrencyUnlimited before
// treating the result as a number of tasks; a bare "cap == 0 means unlimited"
// test would turn a configured overnight pause into no pause at all.
const ConcurrencyUnlimited = -1

// CapSourceOverride is the source the concurrency status surface states
// while an operator's runtime cap override (issue #264, set through
// /api/concurrency) rules. The decision lives in the server — the
// resolver never produces this value, the server overwrites Source with
// it — but the label lives beside the source strings ConcurrencyStatus
// itself produces, so the vocabulary that tells an operator where their
// cap came from has one address.
const CapSourceOverride = "manual override"

// ConcurrencyScheduleEntry is one boundary of the daily concurrency
// schedule (issue #248).
type ConcurrencyScheduleEntry struct {
	// At is the wall-clock time the entry takes effect, in strict
	// 24-hour "HH:MM" form, interpreted in the config's schedule_timezone.
	At string `yaml:"at"`
	// Concurrency is the cap in force from At until the next entry. Unlike
	// max_concurrent_tasks, where 0 means unlimited, 0 here is a real cap:
	// no new work may start. EffectiveConcurrency keeps the two apart by
	// reporting the uncapped case as ConcurrencyUnlimited.
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
		LogRetentionDays:      14,  // prune task logs older than two weeks
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
	if err := cfg.ValidateTrustedHosts(); err != nil {
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

// NormaliseHostName reduces a Host header value, or a configured
// trusted_hosts entry, to the one comparable name both halves of the guard
// match on (issue #286): port stripped, a balanced pair of IPv6 brackets
// stripped, one trailing dot (the absolute-form root label) stripped, case
// folded. It lives here beside the validator below so the guard in
// internal/server and the config side cannot disagree about what a name is,
// and it is idempotent: running it over an already-normalised name changes
// nothing.
//
// Brackets are stripped only when balanced. net.SplitHostPort already yields
// "::1" for the bracketed-with-port form ("[::1]:8080"), so an unbalanced pair
// is a malformed Host value rather than a name, and inventing a name for it
// would admit shapes the address parser itself refuses.
func NormaliseHostName(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if len(host) >= 2 && strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	host = strings.TrimSuffix(host, ".")
	return strings.ToLower(host)
}

// trustedHostNameRe matches a normalised name a client could actually present
// in a Host header: dot-separated labels of letters, digits, hyphens and
// underscores, each starting and ending with a letter, digit or underscore.
// (Underscores are not legal in a registered domain but are common in local
// DNS, and such a name can still be sent.) Anything else—"*.example.com",
// "https://hotelier.example.com", a stray space—names nothing a client speaks,
// so it can only be a typo.
var trustedHostNameRe = regexp.MustCompile(`^[a-z0-9_]([a-z0-9_-]*[a-z0-9_])?(\.[a-z0-9_]([a-z0-9_-]*[a-z0-9_])?)*$`)

// ValidateTrustedHosts checks the trusted_hosts list (issue #286): an entry
// whose normalised form is neither an IP literal nor a name a client could
// present is a configuration error, never silently ignored. An entry that can
// match nothing keeps POST /api/concurrency answering 403 for the life of the
// process—the very failure this field exists to fix, but now looking like a
// broken guard rather than a typo, with no effective list to compare the
// refused Host against.
//
// Wildcard *policy* is issue #285's to decide; this covers only the case where
// an entry cannot match anything, which needs no policy.
//
// LoadServerConfig calls it, and so does every path that installs a config
// into a running server (Server.New / Server.Reload), the way
// ValidateConcurrency does.
func (c ServerConfig) ValidateTrustedHosts() error {
	for _, entry := range c.TrustedHosts {
		if strings.Contains(entry, "/") {
			return fmt.Errorf("trusted_hosts entry %q is not a host name: a scheme or a path does not belong in it, so it would match no Host header (write the host name alone)", entry)
		}
		name := NormaliseHostName(entry)
		if name == "" {
			return fmt.Errorf("trusted_hosts entry %q is empty: it would admit no Host at all", entry)
		}
		if net.ParseIP(name) != nil || trustedHostNameRe.MatchString(name) {
			continue
		}
		if strings.Contains(name, "*") {
			return fmt.Errorf("trusted_hosts entry %q is a wildcard: entries are exact names, and a wildcard would admit whatever name a rebound zone serves", entry)
		}
		return fmt.Errorf("trusted_hosts entry %q (normalised to %q) is neither an exact host name nor an IP address, so no Host header could ever match it", entry, name)
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
// max_concurrent_tasks when no schedule is configured. Entries need not be
// sorted.
//
// The result is a cap on how many tasks may start, and the three answers a
// caller can get mean:
//
//   - ConcurrencyUnlimited: no cap is in force, work starts freely. This is
//     what an unconfigured deployment resolves to, since max_concurrent_tasks
//     defaults to 0 and 0 means unlimited in that key.
//   - 0: no new task may start. A schedule entry expresses this overnight
//     pause with concurrency: 0.
//   - n > 0: at most n tasks may be in flight.
//
// Callers must test ConcurrencyUnlimited first; comparing only for 0 would
// read an overnight pause as "unlimited".
//
// A schedule this function cannot trust (an unparsable "at", an unloadable
// zone) resolves to the static cap rather than to a guess. ValidateConcurrency
// rejects such a config at load and reload time, so reaching those branches
// means the config was built in code without ever being validated.
//
// ConcurrencyStatus is this same resolution with the provenance and the next
// boundary attached, for callers that have to say more than the number.
// This function resolves that whole status and keeps only the cap: the
// next-change walk inside it is work this caller discards. That is
// invisible at the call sites this has (the sync tick, startup, reload),
// but a caller that only ever needs the number on a hot path would pay
// for the walk here rather than resolve the cap alone.
func (c ServerConfig) EffectiveConcurrency(now time.Time) int {
	return c.ConcurrencyStatus(now).Cap
}

// ConcurrencyStatus is the concurrency picture at the instant it was
// resolved (issue #250): the cap in force, which part of the configuration
// produced it, and the next boundary at which the number changes. It is what
// the status surface reports, so an operator can read "the cap is 2 because of
// the 07:00 entry and it rises to 4 at 23:00" off the running server instead
// of off the config file.
//
// Cap follows the same three-way vocabulary as EffectiveConcurrency, which
// this resolves through — the two can never state different caps.
type ConcurrencyStatus struct {
	// Cap is the cap in force: ConcurrencyUnlimited, 0 for a full pause,
	// or a number of tasks.
	Cap int
	// Source says where Cap came from: `concurrency_schedule at "07:00"`
	// naming the entry in force, or "max_concurrent_tasks" when no schedule
	// applies — including when a schedule exists but the resolver cannot
	// trust it, which is a configuration error ValidateConcurrency reports.
	// A runtime override is not the resolver's concern: the server
	// overwrites this with CapSourceOverride after resolving (issue #264).
	Source string
	// Timezone is the zone the boundaries are quoted in, empty when Cap
	// came from the static cap and no wall clock is involved.
	Timezone string
	// NextChangeAt is the instant the cap next changes; zero when it will
	// not change, which is the answer for a static cap and for a schedule
	// whose entries all carry the same cap. A boundary that repeats the
	// current cap is not a change.
	NextChangeAt time.Time
	// NextCap is the cap in force from NextChangeAt.
	NextCap int
}

// HasNextChange reports whether the cap is scheduled to change.
func (s ConcurrencyStatus) HasNextChange() bool { return !s.NextChangeAt.IsZero() }

// NextAt is the wall-clock "HH:MM" of NextChangeAt in Timezone — the
// rendering of a fact the status already carries, derived so no edit can
// make the string and the instant disagree. Empty when there is no change
// to report.
func (s ConcurrencyStatus) NextAt() string {
	if s.NextChangeAt.IsZero() {
		return ""
	}
	loc, err := loadLocation(s.Timezone)
	if err != nil {
		return ""
	}
	return s.NextChangeAt.In(loc).Format("15:04")
}

// ConcurrencyStatus resolves the cap in force at now together with the entry
// it came from and the next change. See EffectiveConcurrency for what the
// cap values mean.
func (c ServerConfig) ConcurrencyStatus(now time.Time) ConcurrencyStatus {
	entries, loc, chosen, ok := c.resolveSchedule(now)
	if !ok {
		return ConcurrencyStatus{Cap: c.staticConcurrency(), Source: "max_concurrent_tasks"}
	}

	status := ConcurrencyStatus{
		Cap:      entries[chosen].value,
		Source:   fmt.Sprintf("concurrency_schedule at %q", entries[chosen].at),
		Timezone: loc.String(),
	}
	status.NextChangeAt, status.NextCap = nextScheduleChange(entries, loc, now, status.Cap)
	return status
}

// scheduleEntry is one boundary of the schedule after parsing, sorting and
// de-duplication.
type scheduleEntry struct {
	at      string
	minutes int
	value   int
}

// resolveSchedule reads the schedule at now and reports the entries sorted by
// minute, the zone they are interpreted in, and the index of the entry in
// force. ok is false when the schedule cannot be trusted (no entries, an
// unparsable "at", an unloadable zone) and the caller must fall back to the
// static cap rather than to a guess.
func (c ServerConfig) resolveSchedule(now time.Time) ([]scheduleEntry, *time.Location, int, bool) {
	if len(c.ConcurrencySchedule) == 0 {
		return nil, nil, 0, false
	}

	entries := make([]scheduleEntry, 0, len(c.ConcurrencySchedule))
	for _, e := range c.ConcurrencySchedule {
		m, err := parseScheduleAt(e.At)
		if err != nil {
			// Dropping the entry would silently hand its window to a
			// neighbouring cap, so fall back to the static cap instead.
			return nil, nil, 0, false
		}
		entries = append(entries, scheduleEntry{at: e.At, minutes: m, value: e.Concurrency})
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].minutes < entries[j].minutes })

	// The search below keeps the last entry at or before now, so two entries
	// for the same minute would otherwise resolve to whichever order the sort
	// happened to produce. ValidateConcurrency rejects a duplicated "at", but
	// a config built in code can still carry one (server.New only logs such a
	// failure), so collapse the duplicates here: the first entry listed for a
	// minute wins.
	deduped := make([]scheduleEntry, 0, len(entries))
	for i, e := range entries {
		if i > 0 && entries[i-1].minutes == e.minutes {
			continue
		}
		deduped = append(deduped, e)
	}
	entries = deduped

	loc, err := loadLocation(c.ScheduleTimezone)
	if err != nil {
		// Evaluating the schedule in UTC would shift every boundary by the
		// zone's offset; the static cap is the honest answer.
		return nil, nil, 0, false
	}
	local := now.In(loc)
	chosen := chosenEntry(entries, local.Hour()*60+local.Minute())
	return entries, loc, chosen, true
}

// chosenEntry is the entry in force at a wall clock: the last entry at or
// before that minute, wrapping around to the last entry of the day before
// the first one. resolveSchedule and nextScheduleChange share the rule, so
// the cap the status surface quotes and the cap enforcement obeys at an
// instant cannot drift apart.
func chosenEntry(entries []scheduleEntry, wallMinutes int) int {
	chosen := len(entries) - 1
	for i, e := range entries {
		if e.minutes <= wallMinutes {
			chosen = i
		}
	}
	return chosen
}

// nextScheduleChange finds the next instant after now at which the cap
// differs from the cap in force. The schedule repeats daily and a cap that
// differs from the current one recurs within one repetition, so a horizon
// of two days of instants always contains any real change (a fall-back day
// is 25 hours long, which the extra day covers).
//
// The horizon is walked on the instant axis, minute by minute, with the
// wall-clock rule of chosenEntry applied at each step — the returned
// instant is by construction the instant EffectiveConcurrency flips. Wall
// clocks cannot be walked instead: on a fall-back day the shifted hour
// occurs twice, and time.Date resolves its ambiguous minute to the later
// occurrence, so the first pass would never become a candidate; and every
// entry inside a spring-forward gap normalises onto the same instant,
// leaving the quoted cap to an unstable sort rather than to the rule
// enforcement obeys. Walking instants visits the repeated hour twice and
// the gap once, and quotes at each step the cap resolveSchedule gives
// there — an entry in a gap is thus placed at the transition, which is
// where the wall-clock comparison flips.
func nextScheduleChange(entries []scheduleEntry, loc *time.Location, now time.Time, cap int) (time.Time, int) {
	end := now.Add(48 * time.Hour)
	for t := now.Truncate(time.Minute).Add(time.Minute); t.Before(end); t = t.Add(time.Minute) {
		if c := capAt(entries, t.In(loc)); c != cap {
			return t, c
		}
	}
	return time.Time{}, cap
}

// capAt is the cap chosenEntry names at a wall clock, for callers holding
// the instant rather than the minute.
func capAt(entries []scheduleEntry, local time.Time) int {
	return entries[chosenEntry(entries, local.Hour()*60+local.Minute())].value
}

// staticConcurrency states the static cap in the resolver's vocabulary:
// max_concurrent_tasks follows the max_guests convention where 0 means
// unlimited, and ConcurrencyUnlimited is how the resolver says that without
// colliding with the 0 a schedule entry uses for a full pause.
func (c ServerConfig) staticConcurrency() int {
	if c.MaxConcurrentTasks == 0 {
		return ConcurrencyUnlimited
	}
	return c.MaxConcurrentTasks
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
