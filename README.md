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
    <a href="https://github.com/sepp-org/sepp/actions"><img alt="CI" src="https://badgers.space/github/checks/sepp-org/sepp/master?label=CI&label_color=181512"></a>
    <a href="https://crates.io/crates/sepp"><img alt="crates.io" src="https://badgers.space/crates/version/sepp?label_color=181512"></a>
    <a href="https://crates.io/crates/sepp"><img alt="downloads" src="https://badgers.space/crates/downloads/sepp?label_color=181512"></a>
    <a href="LICENSE"><img alt="license" src="https://badgers.space/github/license/sepp-org/sepp?label_color=181512&color=ec6a2e"></a>
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
- Built-in web admin UI: live dashboards, job inspection, dead-letter management and an audit log.
- Durable by default. If using the `sync_all` persist mode, all successful operations are guaranteed to be `fsync`-ed.
- End-to-end distributed OpenTelemetry tracing across clients and server. The client SDKs inject trace context automatically.
- OpenTelemetry/Prometheus metrics.
- Clients and server talk gRPC. Bring your own client if you wish.

## Benchmarks

With every operation fsync-ed before it is acknowledged, sepp sustains 1.4x the throughput of BullMQ, roughly 10x beanstalkd and 20x NATS JetStream (200,000 jobs, 256 byte payloads, 50 producers and 50 workers, no batching). Sepp also supports batch enqueue and drain operation, which can dramatically increase throughput even further.

| broker | enqueue jobs/s | drain jobs/s |
|---|---:|---:|
| sepp | 15,325 | 6,980 |
| beanstalkd | 1,494 | 1,431 |
| Faktory | - | - |
| NATS JetStream | 716 | 724 |
| BullMQ | 10,939 | 5,566 |

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

By default, sepp listens on `0.0.0.0:50051` with API-key authentication disabled, and persists data to a `sepp-data` directory in the current working directory. Restrict `listen_addr` or configure `auth` before exposing the port beyond your machine. You can generate a config file to edit with `sepp config example > sepp.toml`. Sepp will pick it up automatically as long as it is named `sepp.toml` and in the working directory the server is started from, or you can specify a custom path with the `SEPP_CONFIG` environment variable. Specifying a custom path is also possible via the `--config` CLI flag.

The admin UI is accessible at [http://localhost:9465](http://localhost:9465). Run locally it binds to loopback and requires no login; the Docker image ships a demo login (`admin`/`admin`) instead. Change both before exposing the port on a production network.

### Client SDKs

sepp has official clients for [Rust](https://github.com/sepp-org/sepp-rs), [Python](https://github.com/sepp-org/sepp-py) and [Node.js](https://github.com/sepp-org/sepp-js). See your preferred language's client SDK for usage instructions.

## Docs

sepp has in-depth documentation on its [docs site](https://sepp-org.github.io/sepp/docs/). The docs include guides for running sepp in production, configuring it and even building your own client.

## License

sepp is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
