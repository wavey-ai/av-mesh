# syntax=docker/dockerfile:1

ARG RUST_IMAGE=rust:1-bookworm

FROM ${RUST_IMAGE} AS builder
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl git pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cargo build --release --locked --bin av-mesh
RUN rustup target add wasm32-unknown-unknown \
  && cargo install trunk --version 0.21.14 --locked \
  && cd dashboard \
  && env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /app/dashboard-dist

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/av-mesh /usr/local/bin/av-mesh
COPY --from=builder /app/dashboard-dist /opt/av-mesh/dashboard

ENV AV_MESH_DASHBOARD_DIST=/opt/av-mesh/dashboard
ENTRYPOINT ["/usr/local/bin/av-mesh"]
