package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"hotelier/pkg/agent"
	"hotelier/pkg/config"
)

func main() {
	configPath := flag.String("config", "config/agent.yaml", "path to agent configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadAgentConfig(*configPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("config file not found, using defaults")
			cfg = config.DefaultAgentConfig()
		} else {
			log.Fatalf("failed to load config: %v", err)
		}
	}

	// Create handler based on task mode
	var handler agent.Handler
	var cleanup func()
	switch cfg.TaskMode {
	case "pi":
		handler, cleanup = createPIHandler(cfg)
	default:
		handler = createShellHandler(cfg)
	}

	// Create agent
	ag := agent.New(cfg, handler)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := ag.Start(); err != nil {
			log.Fatalf("agent error: %v", err)
		}
	}()

	log.Println("agent started")

	// Wait for shutdown signal
	<-sigCh
	log.Println("shutting down agent...")

	// Clean up handler resources (e.g., pi subprocess)
	if cleanup != nil {
		cleanup()
	}

	ag.Stop()

	log.Println("agent stopped")
}

func createShellHandler(cfg config.AgentConfig) agent.Handler {
	return func(ctx context.Context, task agent.TaskAssignment, _ agent.LogCallback) (*agent.TaskResult, error) {
		return agent.DefaultHandler(ctx, task, nil)
	}
}

func createPIHandler(cfg config.AgentConfig) (agent.Handler, func()) {
	workDir := cfg.WorkingDir
	if workDir == "" {
		workDir = "/tmp/hotelier"
	}

	h := agent.NewPIHandler(workDir, "", "", "")
	if err := h.Start(context.Background()); err != nil {
		log.Fatalf("failed to start pi handler: %v", err)
	}

	handler := func(ctx context.Context, task agent.TaskAssignment, sendLog agent.LogCallback) (*agent.TaskResult, error) {
		return h.ExecuteTask(ctx, task, sendLog)
	}

	cleanup := func() {
		log.Println("stopping pi handler...")
		h.Stop(context.Background())
		log.Println("pi handler stopped")
	}

	return handler, cleanup
}
