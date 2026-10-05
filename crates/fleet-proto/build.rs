fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["proto/stage/v1/stage.proto", "proto/fleet/v1/fleet.proto"],
            &["proto"],
        )?;
    Ok(())
}
