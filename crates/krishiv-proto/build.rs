fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc);

    let proto_files = [
        "proto/krishiv/transport/v1/coordinator_executor.proto",
        "proto/spark/connect/base.proto",
    ];

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(prost_config, &proto_files, &["proto"])?;

    for path in proto_files {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-changed=proto/spark/connect");
    Ok(())
}
