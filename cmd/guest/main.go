package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"hotelier/pkg/config"
	"hotelier/pkg/guest"
)

func main() {
	configPath := flag.String("config", "config/guest.yaml", "path to guest configuration file")
	debug := flag.Bool("debug", false, "enable RPC debug logging to stdout")
	flag.Parse()

	// Allow DEBUG env var to override the flag.
	if os.Getenv("DEBUG") == "1" {
		*debug = true
	}

	// Load configuration
	cfg, err := config.LoadGuestConfig(*configPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("config file not found, using defaults")
			cfg = config.DefaultGuestConfig()
		} else {
			log.Fatalf("failed to load config: %v", err)
		}
	}

	// Create the PI handler — the only execution mode for this guest
	handler, cleanup := createPIHandler(cfg, *debug)

	// Create guest
	g := guest.New(cfg, handler)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := g.Start(); err != nil {
			log.Fatalf("guest error: %v", err)
		}
	}()

	log.Println("guest started")

	// Wait for shutdown signal
	<-sigCh
	log.Println("shutting down guest...")

	// Clean up handler resources (pi subprocess)
	cleanup()

	g.Stop()

	log.Println("guest stopped")
}

func createPIHandler(cfg config.GuestConfig, debug bool) (guest.Handler, func()) {
	workDir := cfg.WorkingDir
	if workDir == "" {
		workDir = "/tmp/hotelier"
	}

	h := guest.NewPIHandlerDebug(workDir, "", "", "", debug)
	if err := h.Start(context.Background()); err != nil {
		log.Fatalf("failed to start pi handler: %v", err)
	}

	handler := func(ctx context.Context, task guest.TaskAssignment, sendLog guest.LogCallback) (*guest.TaskResult, error) {
		return h.ExecuteTask(ctx, task, sendLog)
	}

	cleanup := func() {
		log.Println("stopping pi handler...")
		h.Stop(context.Background())
		log.Println("pi handler stopped")
	}

	return handler, cleanup
}
