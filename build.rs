fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Proto files are vendored into `proto/` (committed to the repo). To refresh
    // to the latest published contract, re-run:
    //
    //     buf export buf.build/sepp-org/sepp-proto -o proto
    let includes = &["proto"];
    let protos = &["proto/sepp/v1/queue.proto"];

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(protos, includes)?;

    tonic_prost_build::configure()
        .build_client(true) // For the smoke test.
        .build_server(true)
        .compile_fds(file_descriptors)?;

    Ok(())
}
