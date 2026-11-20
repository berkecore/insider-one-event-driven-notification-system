.PHONY: help build run test test-race test-integration cover vet lint up down logs clean

APP_NAME := notification-server
BIN_DIR := bin

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build server and mockprovider binaries
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/server
	go build -o $(BIN_DIR)/mockprovider ./cmd/mockprovider

run: ## Run notification server locally
	go run ./cmd/server

test: ## Run unit & component test suite with race detector
	go test -race -count=1 ./...

test-integration: ## Run integration tests against live Postgres & Redis
	go test -race -count=1 -tags=integration -v ./tests/integration/...

cover: ## Generate test coverage report
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

vet: ## Run go vet across all packages
	go vet ./...

lint: ## Run golangci-lint (if installed) or fallback to go vet
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; running go vet"; \
		go vet ./...; \
	fi

up: ## Start full stack with Docker Compose
	docker compose up -d --build

down: ## Stop Docker Compose stack
	docker compose down -v

logs: ## Tail application logs from Docker Compose
	docker compose logs -f app

clean: ## Clean build and coverage artifacts
	rm -rf $(BIN_DIR) coverage.out coverage.html
