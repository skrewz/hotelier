package config

import (
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
	// HeartbeatInterval is how often agents must send heartbeats (seconds).
	HeartbeatInterval int `yaml:"heartbeat_interval"`
	// SilenceTimeout is the duration of RPC silence before killing a running task.
	// When an agent stops sending heartbeats for this long, the server kills its
	// pi subprocess and marks the task as failed. Set to 0 to disable.
	SilenceTimeout int `yaml:"silence_timeout"`
	// MaxAgents is the maximum number of agents allowed (0 = unlimited).
	MaxAgents int `yaml:"max_agents"`
	// LogDir is the base directory where task logs are persisted to disk.
	// When empty, logs are kept in memory only.
	LogDir string `yaml:"log_dir"`
}

// AgentConfig holds the configuration for an agent.
type AgentConfig struct {
	// Host is the address of the Check-In Host.
	Host string `yaml:"host"`
	// Port is the port of the Check-In Host.
	Port int `yaml:"port"`
	// ID is the unique identifier for this agent.
	ID string `yaml:"id"`
	// Name is a human-readable name for this agent.
	Name string `yaml:"name"`
	// Tags are the capabilities this agent declares.
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
	// AutoClaimNextTask controls whether the agent automatically picks up
	// the next pending task after completing the current one. When false,
	// the agent remains idle and waits for the host to assign a task.
	AutoClaimNextTask bool `yaml:"auto_claim_next_task"`
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
		MaxAgents:         0,    // unlimited
	}
}

// DefaultAgentConfig returns an AgentConfig with sensible defaults.
func DefaultAgentConfig() AgentConfig {
	return AgentConfig{
		Host:              "localhost",
		Port:              8080,
		TaskTimeout:       0, // use server default
		HeartbeatInterval: 0, // use server default
		LogLevel:          "info",
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

// LoadAgentConfig reads and parses an agent config file.
func LoadAgentConfig(path string) (AgentConfig, error) {
	cfg := DefaultAgentConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
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
