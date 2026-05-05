package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hotelier/internal/server"
	"hotelier/pkg/config"
)

func main() {
	configPath := flag.String("config", "config/server.yaml", "path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadServerConfig(*configPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("config file not found, using defaults")
			cfg = config.DefaultServerConfig()
		} else {
			log.Fatalf("failed to load config: %v", err)
		}
	}

	// Create and start server
	srv := server.New(cfg)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()

	log.Println("hotelier started")

	// Wait for shutdown signal
	<-sigCh
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}

	log.Println("hotelier stopped")
}
