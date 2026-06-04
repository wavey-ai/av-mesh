SHELL := /bin/sh

CARGO ?= cargo
MAKE ?= make

RUST_LOG ?= info
HOST ?= local.bitneedle.com
STREAM_ID ?= 1
PART_MS ?= 50

TLS_DIR ?= ../tls/$(HOST)
CERT ?= $(TLS_DIR)/fullchain.pem
KEY ?= $(TLS_DIR)/privkey.pem
DASHBOARD_DIST ?= $(CURDIR)/dashboard/dist

MESH_ARGS ?=
STACK_ARGS ?=

.DEFAULT_GOAL := help

.PHONY: help service service-release uk us dashboard-build dashboard-serve \
	dashboard-check dashboard-clean local-stack local-stack-debug local-stack-fast \
	k3d-up k3d-check k3d-down build build-release fmt test check

help:
	@printf '%s\n' 'av-mesh tasks'
	@printf '%s\n' ''
	@printf '%s\n' '  make service           Run one local mesh node with dashboard dist'
	@printf '%s\n' '  make service-release   Run one local mesh node in release mode'
	@printf '%s\n' '  make uk                Run the local UK mesh node defaults'
	@printf '%s\n' '  make us                Run the local US mesh node defaults'
	@printf '%s\n' '  make dashboard-build   Build the Leptos dashboard dist'
	@printf '%s\n' '  make dashboard-serve   Serve the Leptos dashboard with Trunk'
	@printf '%s\n' '  make local-stack       Run release local OBS stack via ../av-contrib'
	@printf '%s\n' '  make local-stack-fast  Run existing release stack binaries via ../av-contrib'
	@printf '%s\n' '  make k3d-up           Build and run a two-node local k3d mesh'
	@printf '%s\n' '  make k3d-check        Probe the k3d mesh port-forwards'
	@printf '%s\n' '  make k3d-down         Delete the local k3d mesh cluster'
	@printf '%s\n' '  make test              Run cargo test --locked'
	@printf '%s\n' ''
	@printf '%s\n' 'Common overrides: STREAM_ID=1 PART_MS=50 RUST_LOG=info HOST=local.bitneedle.com'

service:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) AV_MESH_DASHBOARD_DIST=$(DASHBOARD_DIST) \
	$(CARGO) run --bin av-mesh -- \
		--cert $(CERT) \
		--key $(KEY) \
		--stream-id $(STREAM_ID) \
		--part-ms $(PART_MS) \
		$(MESH_ARGS)

service-release:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) AV_MESH_DASHBOARD_DIST=$(DASHBOARD_DIST) \
	$(CARGO) run --bin av-mesh --release -- \
		--cert $(CERT) \
		--key $(KEY) \
		--stream-id $(STREAM_ID) \
		--part-ms $(PART_MS) \
		$(MESH_ARGS)

uk:
	$(MAKE) service-release MESH_ARGS="--region uk --node-id uk-local --mesh-bind 127.0.0.1:29101 --peer 127.0.0.1:29201 --http-port 19444 --playback-base-url https://$(HOST):19444/live --fec-bind 127.0.0.1:22001 --media-fec-bind 127.0.0.1:22101 --telemetry-bind 127.0.0.1:27300 --telemetry-peer 127.0.0.1:27301 --telemetry-dns-name $(HOST) --telemetry-interval-ms 250 --parts-per-segment 2 --window-parts 24 --slot-kb 2048"

us:
	$(MAKE) service-release MESH_ARGS="--region us --node-id us-local --mesh-bind 127.0.0.1:29201 --peer 127.0.0.1:29101 --http-port 19445 --playback-base-url https://$(HOST):19445/live --fec-bind 127.0.0.1:22002 --media-fec-bind 127.0.0.1:22102 --telemetry-bind 127.0.0.1:27301 --telemetry-peer 127.0.0.1:27300 --telemetry-dns-name $(HOST) --telemetry-interval-ms 250 --parts-per-segment 2 --window-parts 24 --slot-kb 2048"

dashboard-build:
	$(MAKE) -C dashboard build DIST=$(DASHBOARD_DIST)

dashboard-serve:
	$(MAKE) -C dashboard serve

dashboard-check:
	$(MAKE) -C dashboard check

dashboard-clean:
	$(MAKE) -C dashboard clean

local-stack:
	$(MAKE) -C ../av-contrib stack HOST=$(HOST) STREAM_ID=$(STREAM_ID) PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) STACK_ARGS="$(STACK_ARGS)"

local-stack-debug:
	$(MAKE) -C ../av-contrib stack-debug HOST=$(HOST) STREAM_ID=$(STREAM_ID) PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) STACK_ARGS="$(STACK_ARGS)"

local-stack-fast:
	$(MAKE) -C ../av-contrib stack-fast HOST=$(HOST) STREAM_ID=$(STREAM_ID) PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) STACK_ARGS="$(STACK_ARGS)"

k3d-up:
	./scripts/k3d-smoke.sh up

k3d-check:
	./scripts/k3d-smoke.sh check

k3d-down:
	./scripts/k3d-smoke.sh down

build:
	$(CARGO) build --locked

build-release:
	$(CARGO) build --locked --release

fmt:
	$(CARGO) fmt

test:
	$(CARGO) test --locked

check:
	$(CARGO) check --locked
