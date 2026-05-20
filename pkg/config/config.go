package config

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"

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
	// SilenceTimeout is the duration of RPC silence before killing a running task.
	// When a guest stops sending heartbeats for this long, the server kills its
	// pi subprocess and marks the task as failed. Set to 0 to disable.
	SilenceTimeout int `yaml:"silence_timeout"`
	// MaxGuests is the maximum number of guests allowed (0 = unlimited).
	MaxGuests int `yaml:"max_guests"`
	// LogDir is the base directory where task logs are persisted to disk.
	// When empty, logs are kept in memory only.
	LogDir string `yaml:"log_dir"`
}

// GuestConfig holds the configuration for a guest.
type GuestConfig struct {
	// Host is the address of the Check-In Host.
	Host string `yaml:"host"`
	// Port is the port of the Check-In Host.
	Port int `yaml:"port"`
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
	// AutoClaimNextTask controls whether the guest automatically picks up
	// the next pending task after completing the current one. When false,
	// the guest remains idle and waits for the host to assign a task.
	AutoClaimNextTask bool `yaml:"auto_claim_next_task"`
	// ClientCert is the path to the TLS client certificate for mTLS
	// authentication with the Check-In Host. When set, the guest will
	// present this certificate during the TLS handshake.
	ClientCert string `yaml:"client_cert"`
	// ClientKey is the path to the TLS client private key for mTLS
	// authentication with the Check-In Host. Must be set together
	// with ClientCert.
	ClientKey string `yaml:"client_key"`
}

// DefaultServerConfig returns a ServerConfig with sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Host:              "0.0.0.0",
		Port:              8080,
		ReadTimeout:       30,
		WriteTimeout:      30,
		MaxLogSize:        1024 * 1024, // 1MB
		TaskTimeout:       3600,        // 1 hour
		HeartbeatInterval: 30,
		SilenceTimeout:    1800, // 30 minutes
		MaxGuests:         0,    // unlimited
	}
}

// DefaultGuestConfig returns a GuestConfig with sensible defaults.
func DefaultGuestConfig() GuestConfig {
	return GuestConfig{
		Host:              "localhost",
		Port:              8080,
		TaskTimeout:       0, // use server default
		HeartbeatInterval: 0, // use server default
		LogLevel:          "info",
		ClientCert:        "",
		ClientKey:         "",
	}
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
	return cfg, nil
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
	return cfg, nil
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
