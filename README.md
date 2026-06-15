<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo/sepp-avatar-light-200.png">
    <img alt="sepp" src="docs/logo/sepp-avatar-dark-200.png" width="128" height="128">
  </picture>

  <h1>sepp</h1>

  <p>
    <strong>A small, language-agnostic durable job queue.</strong>
    <br/>
    Built on <a href="https://github.com/fjall-rs/fjall">fjall</a>, sepp offers fully durable queue operations whilst maintaining high throughput.
  </p>

  <p>
    <a href="https://github.com/sepp-org/sepp/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sepp-org/sepp/ci.yml?branch=master&labelColor=181512"></a>
    <a href="LICENSE"><img alt="license" src="https://img.shields.io/crates/l/sepp.svg?color=ec6a2e&labelColor=181512"></a>
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

## Functionality

- At-least-once delivery via job leasing.
- Exactly-once enqueue via idempotency keys.
- Atomic and best-effort job enqueuing.
- Batch operations for maximum throughput.
- Bring your own payload format. Sepp only sees opaque bytes with an additional encoding hint.

## Operational

- Single static Rust binary, embedded storage.
- Durable by default. If using the `sync_all` persist mode, all operations are guaranteed to be `fsync`-ed.
- End-to-end distributed OpenTelemetry tracing across clients and server. The client SDKs inject trace context automatically.
- OpenTelemetry metrics.
- Clients and server talk gRPC. Bring your own client if you wish.

---

## Building

```sh
cargo build
cargo test
```

Request validation rules are documented as comments in
[`proto/sepp/v1/queue.proto`](proto/sepp/v1/queue.proto) and enforced by the
server in [`src/validate.rs`](src/validate.rs).
