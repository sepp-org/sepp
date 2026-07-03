<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sepp-org/sepp/HEAD/docs/logo/sepp-avatar-light-200.png">
    <img alt="sepp" src="https://raw.githubusercontent.com/sepp-org/sepp/HEAD/docs/logo/sepp-avatar-dark-200.png" width="128" height="128">
  </picture>

  <h1>sepp</h1>

  <p>
    <strong>A small, language-agnostic durable job queue.</strong>
    <br/>
    Built on <a href="https://github.com/fjall-rs/fjall">fjall</a>, sepp offers fully durable queue operations whilst maintaining high throughput.
  </p>

  <p>
    <a href="https://github.com/sepp-org/sepp/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sepp-org/sepp/ci.yml?branch=master&labelColor=181512"></a>
    <a href="https://crates.io/crates/sepp"><img alt="crates.io" src="https://img.shields.io/crates/v/sepp?labelColor=181512"></a>
    <a href="https://crates.io/crates/sepp"><img alt="downloads" src="https://img.shields.io/crates/d/sepp?labelColor=181512"></a>
    <a href="LICENSE"><img alt="license" src="https://img.shields.io/github/license/sepp-org/sepp?color=ec6a2e&labelColor=181512"></a>
    <a href="https://github.com/sepp-org/sepp/stargazers"><img alt="stars" src="https://img.shields.io/github/stars/sepp-org/sepp?style=flat&color=ec6a2e&labelColor=181512"></a>
  </p>

  <p>
    <a href="https://sepp-org.github.io/sepp/docs/get-started/install/">Install</a>
    ·
    <a href="https://buf.build/sepp-org/sepp-proto/docs/main%3Asepp.v1">Protocol</a>
    ·
    <a href="https://sepp-org.github.io/sepp/docs/">Docs</a>
    ·
    <a href="https://github.com/sepp-org/sepp/issues">Issues</a>
  </p>
</div>

> [!WARNING]
> As sepp is pre 1.0, expect bugs and other teething issues when running it in production. Please submit an issue if you encounter any problems.

## Functionality

- At-least-once delivery via job leasing.
- Exactly-once enqueue via idempotency keys.
- Atomic and best-effort job enqueuing.
- Batch operations for maximum throughput.
- Bring your own payload format. Sepp only sees opaque bytes with an additional encoding hint.

## Operational

- Single static Rust binary, embedded storage.
- Durable by default. If using the `sync_all` persist mode, all successful operations are guaranteed to be `fsync`-ed.
- End-to-end distributed OpenTelemetry tracing across clients and server. The client SDKs inject trace context automatically.
- OpenTelemetry/Prometheus metrics.
- Clients and server talk gRPC. Bring your own client if you wish.

## Benchmarks

With every operation fsync-ed before it is acknowledged, sepp sustains roughly 10x the throughput of beanstalkd and 20x NATS JetStream (200,000 jobs, 256 byte payloads, 50 producers and 50 workers, no batching). Sepp also supports batch enqueue and drain operation, which can dramatically increase throughput even further.

| broker | enqueue jobs/s | drain jobs/s |
|---|---:|---:|
| sepp | 15,325 | 6,980 |
| beanstalkd | 1,494 | 1,431 |
| Faktory | - | - |
| NATS JetStream | 716 | 724 |

Faktory has no fully durable mode. Buffered-mode results, concurrency scaling, methodology and hardware are in the [benchmark docs](https://sepp-org.github.io/sepp/docs/benchmarks/).

## Install

Via Cargo:
```sh
cargo install sepp --locked
```

Build from source:
```sh
git clone https://github.com/sepp-org/sepp.git
cd sepp
cargo build --release
```

or grab a binary from the [releases page](https://github.com/sepp-org/sepp/releases).

### Docker

Start sepp with:
```sh
docker run --rm \
  -p 50051:50051 -p 9465:9465 \
  -v sepp-data:/sepp/sepp-data \
  ghcr.io/sepp-org/sepp:latest
```

## Quickstart

By default, sepp listens on `localhost:50051` and persists data to a `sepp-data` directory in the current working directory. You can generate a config file to edit with `sepp config example > sepp.toml`. Sepp will pick it up automatically as long as it is named `sepp.toml` and in the same directory as the binary or you can specify a custom path with the `SEPP_CONFIG` environment variable. Specifying a custom path is also possible via the `--config` CLI flag.

The admin UI is accessible at [http://localhost:9465](http://localhost:9465) with the default login `admin` and key `admin`. Change this before exposing the port on a production network.

### Client SDKs

sepp has official clients for [Rust](TODO), [Python](TODO) and [Node.js](TODO). See your preferred language's client SDK for usage instructions.

## Docs

sepp has in-depth documentation on its [docs site](https://sepp-org.github.io/sepp/docs/). The docs include guides for running sepp in production, configuring it and even building your own client.

## License

sepp is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
