.PHONY: build test test-coverage test-race test-integration lint check-format clean run-server run-guest image

MODULE  := hotelier
GO      := go
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.Version=$(VERSION)

all: build

build: ## Build hotelier server and guest binaries
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/hotelier ./cmd/hotelier
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/guest ./cmd/guest

test: lint test-coverage test-race test-integration ## Run all tests (after linting)
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

test-integration: ## Run integration tests (Playwright UI validation)
	$(GO) test -race -tags=integration -count=1 ./test/integration/
	@echo "Integration tests passed"

lint: ## Run go vet and check formatting with gofumpt
	$(GO) vet ./...
	$(MAKE) check-format

clean: ## Remove build artifacts and container image
	rm -rf bin/ coverage.out coverage.html
	podman rmi hotelier:latest 2>/dev/null || true

run-server: ## Run the hotelier server
	$(GO) run ./cmd/hotelier

run-guest: ## Run the hotelier guest
	$(GO) run ./cmd/guest

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
	$(GO) install -trimpath -ldflags "$(LDFLAGS)" ./cmd/guest

image: ## Build the hotelier container image
	podman build \
		--format docker \
		--build-arg LDFLAGS="$(LDFLAGS)" \
		-t hotelier:latest \
		.

docs/screenshot-candidate.png: bin/hotelier
	@echo "Starting hotelier server..."
	@./bin/hotelier &
	@sleep 2
	@node -e "
	const WebSocket = require('ws');
	const guests = [
	  { id: 'guest-alpha', name: 'Dev Guest Alpha', tags: ['business-default', 'frontend'] },
	  { id: 'guest-beta', name: 'Dev Guest Beta', tags: ['business-default', 'backend'] },
	];
	const connections = [];
	for (const g of guests) {
	  const ws = new WebSocket('ws://localhost:8080/ws');
	  ws.on('open', () => {
	    ws.send(JSON.stringify({
	      jsonrpc: '2.0', method: 'guest.register',
	      params: { id: g.id, name: g.name, tags: g.tags }, id: 1
	    }));
	  });
	  ws.on('message', () => {});
	  ws.on('error', () => {});
	  connections.push(ws);
	}
	setTimeout(async () => {
	  const { chromium } = require('playwright');
	  const browser = await chromium.launch({ headless: true });
	  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
	  await page.goto('http://localhost:8080/');
	  await page.waitForLoadState('networkidle');
	  await page.waitForTimeout(1500);
	  await page.screenshot({ path: 'docs/screenshot-candidate.png' });
	  await browser.close();
	  connections.forEach(c => c.close());
	  console.log('Screenshot saved to docs/screenshot-candidate.png');
	  process.exit(0);
	}, 2000);
	"
	@pkill -f 'bin/hotelier' 2>/dev/null || true


