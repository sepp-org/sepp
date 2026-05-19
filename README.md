# Ideas

Maybe use structurize for schemas??? Need to evaluate this in practice

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
