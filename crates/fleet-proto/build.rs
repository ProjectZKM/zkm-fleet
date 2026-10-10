fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(".fleet.v1.ArtifactRef", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".fleet.v1.ProofInputs", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".fleet.v1.ProofOutputs", "#[derive(serde::Serialize, serde::Deserialize)]")
        .field_attribute(".fleet.v1.ProofOutputs.machine", "#[serde(default)]")
        .type_attribute(".fleet.v1.ProofFetch", "#[derive(serde::Serialize, serde::Deserialize)]")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["proto/stage/v1/stage.proto", "proto/fleet/v1/fleet.proto"],
            &["proto"],
        )?;
    Ok(())
}
