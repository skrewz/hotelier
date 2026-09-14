package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"hotelier/pkg/chroot"
	"hotelier/pkg/config"
	"hotelier/pkg/guest"
)

// reloadableGuestConfig wraps LoadGuestConfig for the config watcher.
func reloadableGuestConfig(path string) (interface{}, error) {
	return config.LoadGuestConfig(path)
}

func main() {
	configPath := flag.String("config", "config/guest.yaml", "path to guest configuration file")
	debug := flag.Bool("debug", false, "enable RPC debug logging to stdout")
	flag.Parse()

	// Allow DEBUG env var to override the flag.
	if os.Getenv("DEBUG") == "1" {
		*debug = true
	}

	// Chroot isolation is unavoidable (issue #51): every task runs inside
	// its own chroot jail, so the guest process must be able to call
	// chroot(2). Fail fast at startup rather than on the first task.
	if ok, err := chroot.CanChroot(); err != nil {
		log.Printf("warning: could not determine chroot capability: %v", err)
	} else if !ok {
		log.Fatalf("chroot isolation is required (issue #51) but this process lacks CAP_SYS_CHROOT; run the guest as root or with --cap-add SYS_CHROOT")
	}

	// Device nodes in the jail (notably /dev/null, which git requires)
	// need CAP_MKNOD. Without it the jail is still built and pi runs, but
	// /dev-dependent tools misbehave inside it — warn rather than fail.
	if ok, err := chroot.CanMknod(); err != nil {
		log.Printf("warning: could not determine mknod capability: %v", err)
	} else if !ok {
		log.Printf("warning: this process lacks CAP_MKNOD; /dev nodes (e.g. /dev/null, needed by git) will be absent from task jails")
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

	// Set up config file watcher for hot-reload
	configCh := make(chan interface{}, 1)
	watcher, err := config.NewConfigWatcher(*configPath, log.Default(), reloadableGuestConfig)
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
		if err := g.Start(); err != nil {
			log.Fatalf("guest error: %v", err)
		}
	}()

	log.Println("guest started")

	// Wait for shutdown signal or config reload
	for {
		select {
		case cfg := <-configCh:
			// Apply the reloaded config directly from the watcher.
			if newCfg, ok := cfg.(config.GuestConfig); ok {
				g.Reload(newCfg)
			} else {
				log.Printf("failed to cast reloaded config to GuestConfig")
			}
		case <-sigCh:
			goto shutdown
		}
	}

shutdown:
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
