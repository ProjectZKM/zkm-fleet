fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut build = tonic_build::configure()
        .btree_map([".fleet.model.v1"])
        .extern_path(".fleet.model.v1.Json", "crate::json::Json");
    for name in [
        "StringList",
        "Quota",
        "Allocation",
        "Usage",
        "Namespace",
        "Token",
        "App",
        "WorkloadTemplate",
        "Job",
        "CommandRecord",
        "NodeReport",
        "JobReport",
        "CommandResult",
        "ScaleStatus",
        "AppDeployment",
        "ScaleRequest",
        "CreateNamespaceRequest",
        "AllocationRequest",
        "NamespaceSelector",
        "CreateTokenRequest",
        "ProofJobsQuery",
        "RegisterAppRequest",
        "AppProof",
        "ScalePhase",
        "SubmitProofRequest",
        "ProofJobFilter",
        "NodeStatus",
        "AppAnchor",
        "FleetStatus",
        "PreemptionCandidate",
        "NodeCapacity",
        "NodePin",
        "PreemptionRequest",
        "PreemptionPlan",
    ] {
        build = build.type_attribute(
            format!(".fleet.model.v1.{name}"),
            "#[derive(serde::Serialize, serde::Deserialize)]",
        );
    }
    for name in [
        "Quota",
        "NodeReport",
        "JobReport",
        "CommandResult",
        "ScaleRequest",
        "CreateTokenRequest",
        "NamespaceSelector",
        "ProofJobsQuery",
        "RegisterAppRequest",
        "AppProof",
        "SubmitProofRequest",
        "PreemptionCandidate",
        "NodeCapacity",
        "PreemptionRequest",
    ] {
        build = build.type_attribute(
            format!(".fleet.model.v1.{name}"),
            "#[serde(deny_unknown_fields)]",
        );
    }
    for path in [
        "Namespace.quota",
        "Namespace.tag_quotas",
        "WorkloadTemplate.removed",
        "Job.logs_ready",
        "Job.purged",
        "CommandRecord.admitted",
        "NodeReport.batches",
        "NodeReport.apps",
        "NodeReport.jobs",
        "NodeReport.results",
        "CreateNamespaceRequest.quota",
        "AllocationRequest.restore",
        "CreateTokenRequest.label",
        "CreateTokenRequest.permissions",
        "AppProof.backends",
        "Token.permissions",
        "ProofJobsQuery.active",
        "ProofJobFilter.active",
        "PreemptionRequest.pins",
        "PreemptionRequest.free",
        "PreemptionRequest.candidates",
    ] {
        build = build.field_attribute(format!(".fleet.model.v1.{path}"), "#[serde(default)]");
    }
    build = build
        .type_attribute(".fleet.model.v1.NodePin", "#[serde(transparent)]")
        .type_attribute(".fleet.model.v1.StringList", "#[serde(transparent)]")
        .type_attribute(
            ".fleet.model.v1.ScalePhase",
            "#[serde(rename_all = \"snake_case\")]",
        )
        .field_attribute(
            ".fleet.model.v1.Token.address",
            "#[serde(skip_serializing)]",
        )
        .field_attribute(
            ".fleet.model.v1.Token.token_hash",
            "#[serde(skip_serializing)]",
        )
        .field_attribute(
            ".fleet.model.v1.ScaleStatus.phase",
            "#[serde(with = \"crate::models::scale_phase\")]",
        )
        .field_attribute(
            ".fleet.model.v1.ProofJobsQuery.limit",
            "#[serde(default = \"crate::models::default_jobs_limit\")]",
        );
    build
        .type_attribute(".fleet.v1.ArtifactRef", "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(".fleet.v1.ProofInputs", "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(".fleet.v1.ProofOutputs", "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .field_attribute(".fleet.v1.ProofOutputs.machine", "#[serde(default)]")
        .type_attribute(".fleet.v1.ProofFetch", "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["proto/stage/v1/stage.proto", "proto/fleet/v1/fleet.proto"],
            &["proto"],
        )?;
    Ok(())
}
