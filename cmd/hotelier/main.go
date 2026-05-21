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

// reloadableConfig wraps LoadServerConfig so it can be passed to config.NewConfigWatcher.
func reloadableConfig(path string) (interface{}, error) {
	return config.LoadServerConfig(path)
}

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

	// Set up config file watcher for hot-reload
	configCh := make(chan interface{}, 1)
	watcher, err := config.NewConfigWatcher(*configPath, log.Default(), reloadableConfig)
	if err != nil {
		log.Printf("failed to create config watcher: %v (config changes will not be auto-reloaded)", err)
	} else {
		go watcher.Run(configCh)
		log.Printf("config watcher active: %s", *configPath)
		defer watcher.Close()
	}

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()

	log.Println("hotelier started")

	// Wait for shutdown signal or config reload
	for {
		select {
		case cfg := <-configCh:
			// Apply the reloaded config directly from the watcher.
			if newCfg, ok := cfg.(config.ServerConfig); ok {
				srv.Reload(newCfg)
			} else {
				log.Printf("failed to cast reloaded config to ServerConfig")
			}
		case <-sigCh:
			goto shutdown
		}
	}

shutdown:
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}

	log.Println("hotelier stopped")
}
