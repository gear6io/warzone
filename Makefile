# warzone dev commands. See docs/development.md for the full guide.
.PHONY: build test fmt lint run dev-up dev-down run-stack clean-data help

COMPOSE := docker compose -f dev/compose.yaml

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

build: ## cargo build the whole workspace
	cargo build --workspace

test: ## Run all tests (infra-free)
	cargo test --workspace

fmt: ## Format the code
	cargo fmt --all

lint: ## Clippy, warnings as errors
	cargo clippy --workspace --all-targets -- -D warnings

run: ## Tier 1: run against the zero-infra local config
	cargo run -- --config dev/local.yaml

dev-up: ## Tier 2: start the Polaris + SeaweedFS stack (bucket + catalog created)
	$(COMPOSE) up -d --wait

dev-down: ## Tier 2: stop the stack and delete its volumes
	$(COMPOSE) down -v

run-stack: ## Tier 2: run against the REST-catalog/S3 stack (needs dev-up first)
	cargo run -- --config dev/stack.yaml

clean: ## Remove Tier 1 local data (dev/data/)
	rm -rf dev/data/

load-dummy:
	go run dev/scripts/ingestion/main.go
