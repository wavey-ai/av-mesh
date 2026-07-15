SHELL := /bin/sh

CARGO ?= cargo
MAKE ?= make
NEEDLETAIL_ROOT ?= ../needletail
MISSION_CONTROL_DIST ?= $(abspath $(NEEDLETAIL_ROOT)/mission-control/dist)

RUST_LOG ?= info
HOST ?= local.bitneedle.com
STREAM_ID ?= 1
PART_MS ?= 50

TLS_DIR ?= ../tls/$(HOST)
CERT ?= $(TLS_DIR)/fullchain.pem
KEY ?= $(TLS_DIR)/privkey.pem
MESH_ARGS ?=
STACK_ARGS ?=

.DEFAULT_GOAL := help

.PHONY: help service service-release uk us mission-control-build mission-control-serve \
	mission-control-check mission-control-clean local-stack local-stack-debug local-stack-fast \
	realtime-benchmark realtime-qualification realtime-soak observability-check \
	k3d-up k3d-check k3d-down build build-release fmt test check

help:
	@printf '%s\n' 'av-mesh tasks'
	@printf '%s\n' ''
	@printf '%s\n' '  make service           Run one local playback edge with Mission Control assets'
	@printf '%s\n' '  make service-release   Run one local mesh node in release mode'
	@printf '%s\n' '  make uk                Run the local UK mesh node defaults'
	@printf '%s\n' '  make us                Run the local US mesh node defaults'
	@printf '%s\n' '  make mission-control-build Build Needletail Mission Control assets'
	@printf '%s\n' '  make mission-control-serve Serve Needletail Mission Control'
	@printf '%s\n' '  make local-stack       Run the Needletail local constellation'
	@printf '%s\n' '  make local-stack-fast  Run Needletail with existing release binaries and product assets'
	@printf '%s\n' '  make realtime-benchmark Benchmark a running contributor + mesh stack'
	@printf '%s\n' '  make realtime-qualification Run baseline + controlled-loss qualification'
	@printf '%s\n' '  make realtime-soak      Run the deployed simultaneous-load soak gate'
	@printf '%s\n' '  make observability-check Validate Prometheus, Alertmanager, and Grafana assets'
	@printf '%s\n' '  make k3d-up           Build and run a two-node local k3d mesh'
	@printf '%s\n' '  make k3d-check        Probe the k3d mesh port-forwards'
	@printf '%s\n' '  make k3d-down         Delete the local k3d mesh cluster'
	@printf '%s\n' '  make test              Run cargo test --locked'
	@printf '%s\n' ''
	@printf '%s\n' 'Common overrides: STREAM_ID=1 PART_MS=50 RUST_LOG=info HOST=local.bitneedle.com'

service:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) NEEDLETAIL_MISSION_CONTROL_DIST=$(MISSION_CONTROL_DIST) \
	$(CARGO) run --bin av-mesh -- \
		--cert $(CERT) \
		--key $(KEY) \
		--stream-id $(STREAM_ID) \
		--part-ms $(PART_MS) \
		$(MESH_ARGS)

service-release:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) NEEDLETAIL_MISSION_CONTROL_DIST=$(MISSION_CONTROL_DIST) \
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

mission-control-build:
	$(MAKE) -C $(NEEDLETAIL_ROOT)/mission-control build DIST=$(MISSION_CONTROL_DIST)

mission-control-serve:
	$(MAKE) -C $(NEEDLETAIL_ROOT)/mission-control serve

mission-control-check:
	$(MAKE) -C $(NEEDLETAIL_ROOT)/mission-control check

mission-control-clean:
	$(MAKE) -C $(NEEDLETAIL_ROOT)/mission-control clean DIST=$(MISSION_CONTROL_DIST)

local-stack:
	$(MAKE) -C $(NEEDLETAIL_ROOT) local HOST=$(HOST) STREAM_ID=$(STREAM_ID) PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) STACK_ARGS="$(STACK_ARGS)"

local-stack-debug:
	$(MAKE) -C $(NEEDLETAIL_ROOT) local-debug HOST=$(HOST) STREAM_ID=$(STREAM_ID) PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) STACK_ARGS="$(STACK_ARGS)"

local-stack-fast:
	$(MAKE) -C $(NEEDLETAIL_ROOT) local-fast HOST=$(HOST) STREAM_ID=$(STREAM_ID) PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) STACK_ARGS="$(STACK_ARGS)"

realtime-benchmark:
	./scripts/realtime-benchmark.sh

realtime-qualification:
	./scripts/realtime-qualification.sh

realtime-soak:
	./scripts/realtime-soak.sh

observability-check:
	./scripts/validate-observability.sh

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
