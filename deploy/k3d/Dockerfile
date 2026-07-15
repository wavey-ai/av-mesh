# syntax=docker/dockerfile:1

ARG RUST_IMAGE=rust:1-bookworm

FROM ${RUST_IMAGE} AS builder
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl git pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cargo build --release --locked --bin av-mesh

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/av-mesh /usr/local/bin/av-mesh
ENTRYPOINT ["/usr/local/bin/av-mesh"]
