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
	debug := flag.Bool("debug", false, "enable RPC debug logging to stdout")
	flag.Parse()

	// Allow DEBUG env var to override the flag.
	if os.Getenv("DEBUG") == "1" {
		*debug = true
	}

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

	// Create the PI handler — the only execution mode for this agent
	handler, cleanup := createPIHandler(cfg, *debug)

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

	// Clean up handler resources (pi subprocess)
	cleanup()

	ag.Stop()

	log.Println("agent stopped")
}

func createPIHandler(cfg config.AgentConfig, debug bool) (agent.Handler, func()) {
	workDir := cfg.WorkingDir
	if workDir == "" {
		workDir = "/tmp/hotelier"
	}

	h := agent.NewPIHandlerDebug(workDir, "", "", "", debug)
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
