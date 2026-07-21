# syntax=docker/dockerfile:1.7

FROM lukemathwalker/cargo-chef:latest-rust-1-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .

RUN cargo build --release --bin sepp \
    && mkdir -p /app/empty-data

# Transform the example config for Docker: bind admin UI to all interfaces
# with a demo key so it is reachable without mounting a custom config.
RUN cp /app/sepp.example.toml /app/sepp.docker.toml \
    && sed -i \
        -e 's/listen_addr = "127\.0\.0\.1:9465"/listen_addr = "0.0.0.0:9465"/' \
        -e '/^#keys = \[/,/^#\]/c\keys = [\n  { name = "admin", key = "admin", role = "admin" },\n]' \
        /app/sepp.docker.toml

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /sepp
COPY --from=builder /app/target/release/sepp /usr/local/bin/sepp
# --chown: the server (uid 65532) rewrites this file for admin-UI config edits,
# and write_atomic() needs to create a .tmp sibling in the directory.
COPY --from=builder --chown=65532:65532 /app/sepp.docker.toml /etc/sepp/sepp.toml
COPY --from=builder --chown=65532:65532 /app/empty-data/ /sepp/sepp-data/
# 50051 gRPC, 9464 Prometheus (off by default), 9465 admin UI
EXPOSE 50051 9464 9465
VOLUME ["/sepp/sepp-data"]
ENV SEPP_CONFIG=/etc/sepp/sepp.toml

ENTRYPOINT ["/usr/local/bin/sepp"]
