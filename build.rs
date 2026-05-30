fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `protoc` must be installed and on PATH (or pointed at by the PROTOC env
    // var). It is a build prerequisite — see the README. We cannot vendor it:
    // a transitive dependency (`prost-protovalidate-types`) also compiles
    // `.proto` files in its own build script and resolves `protoc` the same
    // way, and a build script cannot inject env vars into sibling crates.
    //
    // Proto files are vendored into `proto/` (committed to the repo). The
    // version below is the one currently vendored; to refresh, bump it to a
    // newer published label/commit and re-run:
    //
    //     buf export buf.build/sepp-org/sepp-proto:v1.0.1 -o proto
    //
    // The build itself never invokes `buf` or touches the network, so it works
    // in offline CI and picks up local edits to `proto/sepp/v1/queue.proto`.
    let includes = &["proto"];
    let protos = &["proto/sepp/v1/queue.proto"];

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto/buf/validate/validate.proto");

    let mut prost_config = prost_build::Config::new();

    prost_reflect_build::Builder::new()
        .descriptor_pool("crate::pb::DESCRIPTOR_POOL")
        .configure(&mut prost_config, protos, includes)?;

    tonic_prost_build::configure()
        .build_client(true) // For the smoke test.
        .build_server(true)
        .compile_with_config(prost_config, protos, includes)?;

    Ok(())
}
