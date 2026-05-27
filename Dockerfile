# syntax=docker/dockerfile:1.7

FROM lukemathwalker/cargo-chef:latest-rust-1-bookworm AS chef
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
ENV PROTOC_INCLUDE=/usr/include

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .

RUN cargo build --release --bin sepp \
    && mkdir -p /app/empty-data

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /sepp
COPY --from=builder /app/target/release/sepp /usr/local/bin/sepp
COPY --from=builder /app/sepp.example.toml /etc/sepp/sepp.toml
COPY --from=builder --chown=65532:65532 /app/empty-data/ /sepp/sepp-data/
EXPOSE 50051
VOLUME ["/sepp/sepp-data"]
ENV SEPP_CONFIG=/etc/sepp/sepp.toml

ENTRYPOINT ["/usr/local/bin/sepp"]
