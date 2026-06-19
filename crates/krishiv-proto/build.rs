use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(tmpdir) = std::env::var("TMPDIR") {
        let path = PathBuf::from(tmpdir);
        fs::create_dir_all(&path)?;
    }

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc);

    tonic_prost_build::configure().compile_with_config(
        prost_config,
        &[
            "proto/krishiv/transport/v1/coordinator_executor.proto",
            "proto/krishiv/transport/v1/barrier.proto",
        ],
        &["proto"],
    )?;

    println!("cargo:rerun-if-changed=proto/krishiv/transport/v1/coordinator_executor.proto");
    println!("cargo:rerun-if-changed=proto/krishiv/transport/v1/barrier.proto");
    Ok(())
}
