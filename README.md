<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo/sepp-avatar-dark-200.png">
    <img alt="sepp" src="docs/logo/sepp-avatar-light-200.png" width="128" height="128">
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
    <a href="#building">Install</a>
    ·
    <a href="proto/queue.proto">Protocol</a>
    ·
    <a href="https://docs.rs/sepp">Docs</a>
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

`protoc` (the Protocol Buffers compiler) must be installed and on `PATH`
before building. The build compiles `.proto` files — both sepp's own and
those of a dependency (`prost-protovalidate`) — and there is no way to vendor
`protoc` for the whole dependency graph, so it is a hard build prerequisite.

Install it with your package manager:

| Platform | Command                                  |
| -------- | ---------------------------------------- |
| macOS    | `brew install protobuf`                  |
| Debian   | `apt-get install protobuf-compiler`      |
| Windows  | `winget install protobuf` (or `choco install protoc`) |

Or download a release from <https://github.com/protocolbuffers/protobuf/releases>.

If `protoc` is installed somewhere not on `PATH`, point the `PROTOC`
environment variable at the binary instead. Then:

```sh
cargo build
cargo test
```
