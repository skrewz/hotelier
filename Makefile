.PHONY: build test test-coverage test-race lint clean run-server run-agent help image image-clean

MODULE  := hotelier
GO      := go
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.Version=$(VERSION)

all: build

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build hotelier server and agent binaries
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/hotelier ./cmd/hotelier
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/agent ./cmd/agent

test: ## Run all tests
	$(GO) test -race -count=1 ./...

test-coverage: ## Run tests with coverage report
	$(GO) test -race -count=1 -coverprofile=coverage.out -coverpkg=./pkg/... ./...
	@echo ""
	@$(GO) tool cover -func=coverage.out | grep -E 'total'
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "HTML report: coverage.html"

test-race: ## Run tests with race detector
	$(GO) test -race -count=1 ./...

lint: ## Run go vet and check formatting
	$(GO) vet ./...
	$(MAKE) check-format

clean: ## Remove build artifacts and container image
	rm -rf bin/ coverage.out coverage.html
	$(MAKE) image-clean

run-server: ## Run the hotelier server
	$(GO) run ./cmd/hotelier

run-agent: ## Run the hotelier agent
	$(GO) run ./cmd/agent

fmt: ## Format Go code
	$(GO) fmt ./...

format: ## Format Go code with gofumpt (strict formatting)
	gofumpt -w .

check-format: ## Check Go code formatting with gofumpt (fails if not formatted)
	@if [ -n "$$($(GO) env GOPATH)" ]; then \
		GOFUMPT=$$($(GO) env GOPATH)/bin/gofumpt; \
	else \
		GOFUMPT=gofumpt; \
	fi; \
	files=$$($$GOFUMPT -l .); \
	if [ -n "$$files" ]; then \
		echo "Files not formatted with gofumpt:"; \
		echo "$$files"; \
		exit 1; \
	fi

tidy: ## Tidy go.mod
	$(GO) mod tidy

install: ## Install binaries to $GOPATH/bin
	$(GO) install -trimpath -ldflags "$(LDFLAGS)" ./cmd/hotelier
	$(GO) install -trimpath -ldflags "$(LDFLAGS)" ./cmd/agent

image: ## Build the hotelier container image
	podman build \
		--build-arg LDFLAGS="$(LDFLAGS)" \
		-t hotelier:latest \
		.

image-clean: ## Remove the hotelier container image
	podman rmi hotelier:latest 2>/dev/null || true
