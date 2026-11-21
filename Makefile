# Developer entry points. `make help` lists targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Local runs (outside Docker) talk to the infrastructure published by docker-compose.
export DATABASE_URL ?= postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable
export KAFKA_BROKERS ?= localhost:9092
export REDIS_ADDR ?= localhost:6379
export LOG_FORMAT ?= text

.PHONY: help
help: ## Show this help
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## ---- build & run
.PHONY: build
build: ## Build bin/notifier and bin/mockprovider
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/notifier ./cmd/notifier
	CGO_ENABLED=0 go build -trimpath -o bin/mockprovider ./cmd/mockprovider

.PHONY: run-api run-dispatcher run-worker run-mockprovider
run-api: ## Run the API locally (needs `make infra`)
	go run ./cmd/notifier api
run-dispatcher: ## Run the dispatcher locally (admin on :9090)
	go run ./cmd/notifier dispatcher
run-worker: ## Run a worker locally (admin on :9091)
	ADMIN_ADDR=:9091 go run ./cmd/notifier worker
run-mockprovider: ## Run the mock provider locally on :8081
	go run ./cmd/mockprovider

## ---- quality
.PHONY: test
test: ## Run all tests with the race detector (integration tests need Docker)
	go test -race -count=1 ./...

.PHONY: test-short
test-short: ## Run unit tests only (no Docker)
	go test -race -count=1 -short ./...

.PHONY: cover
cover: ## Run tests with coverage and print the summary
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format code (gofmt + goimports via golangci-lint)
	golangci-lint fmt ./...

.PHONY: tidy
tidy: ## Tidy go.mod / go.sum
	go mod tidy

## ---- docker-compose
.PHONY: up
up: ## Build and start the full stack (API on :8080)
	docker compose up -d --build

.PHONY: up-scaled
up-scaled: ## Start the stack with 3 API and 3 worker replicas (ADR-0008)
	docker compose up -d --build --scale api=3 --scale worker=3

.PHONY: infra
infra: ## Start only infrastructure (postgres, kafka, redis, mock provider)
	docker compose up -d --build postgres kafka kafka-init redis mockprovider

.PHONY: down
down: ## Stop the stack (keeps volumes)
	docker compose down

.PHONY: clean
clean: ## Stop the stack, delete volumes and build output
	docker compose down -v
	rm -rf bin coverage.out

.PHONY: logs
logs: ## Follow logs of the notifier roles
	docker compose logs -f api dispatcher worker

.PHONY: ps
ps: ## Show stack status
	docker compose ps -a
