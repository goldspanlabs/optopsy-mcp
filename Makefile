# Load .env if present
-include .env
export

DATA_ROOT ?= $(HOME)/.optopsy/cache
DB_PATH    = $(DATA_ROOT)/optopsy.db
PORT      ?= 8000

.PHONY: build run dev test lint fmt check reset-db clean

build:
	cargo build --release

run:
	PORT=$(PORT) cargo run --release

dev:
	PORT=$(PORT) cargo run

test:
	cargo test

lint:
	cargo clippy --all-targets

fmt:
	cargo fmt

check: fmt lint test

## Reset the database (strategies will be re-seeded on next start)
reset-db:
	@rm -f "$(DB_PATH)" "$(DB_PATH)-wal" "$(DB_PATH)-shm"
	@echo "Deleted $(DB_PATH) — restart the server to recreate."

## Remove build artifacts and database
clean: reset-db
	cargo clean
