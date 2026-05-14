.PHONY: build test test-coverage test-race lint check-format clean run-server run-agent image

MODULE  := hotelier
GO      := go
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.Version=$(VERSION)

all: build

build: ## Build hotelier server and agent binaries
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/hotelier ./cmd/hotelier
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/agent ./cmd/agent

test: lint test-coverage test-race ## Run all tests (after linting)
	@echo "All test targets passed"

test-coverage: ## Run tests with coverage report
	$(GO) test -race -count=1 -coverprofile=coverage.out -coverpkg=./pkg/... ./...
	@echo ""
	@$(GO) tool cover -func=coverage.out | grep -E 'total'
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "HTML report: coverage.html"
	@echo "Coverage passed"

test-race: ## Run tests with race detector
	$(GO) test -race -count=1 ./...
	@echo "Race detector passed"

lint: ## Run go vet and check formatting with gofumpt
	$(GO) vet ./...
	$(MAKE) check-format

clean: ## Remove build artifacts and container image
	rm -rf bin/ coverage.out coverage.html
	podman rmi hotelier:latest 2>/dev/null || true

run-server: ## Run the hotelier server
	$(GO) run ./cmd/hotelier

run-agent: ## Run the hotelier agent
	$(GO) run ./cmd/agent

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

install: ## Install binaries to $GOPATH/bin
	$(GO) install -trimpath -ldflags "$(LDFLAGS)" ./cmd/hotelier
	$(GO) install -trimpath -ldflags "$(LDFLAGS)" ./cmd/agent

image: ## Build the hotelier container image
	podman build \
		--format docker \
		--build-arg LDFLAGS="$(LDFLAGS)" \
		-t hotelier:latest \
		.


