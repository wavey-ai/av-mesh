SHELL := /bin/sh

CARGO ?= cargo
TRUNK ?= trunk
TRUNK_ENV ?= env -u NO_COLOR TRUNK_COLOR=never

ADDRESS ?= 127.0.0.1
PORT ?= 5188
DIST ?= dist

.DEFAULT_GOAL := help

.PHONY: help serve build check clean

help:
	@printf '%s\n' 'av-mesh dashboard tasks'
	@printf '%s\n' ''
	@printf '%s\n' '  make serve   Serve the Leptos dashboard with Trunk'
	@printf '%s\n' '  make build   Build release dashboard assets'
	@printf '%s\n' '  make check   Cargo-check the WASM target'
	@printf '%s\n' '  make clean   Remove dashboard dist'
	@printf '%s\n' ''
	@printf '%s\n' 'Common overrides: ADDRESS=127.0.0.1 PORT=5188 DIST=dist'

serve:
	$(TRUNK_ENV) $(TRUNK) serve --address $(ADDRESS) --port $(PORT)

build:
	$(TRUNK_ENV) $(TRUNK) build --release --dist $(DIST)

check:
	$(CARGO) check --locked --target wasm32-unknown-unknown

clean:
	rm -rf $(DIST)
