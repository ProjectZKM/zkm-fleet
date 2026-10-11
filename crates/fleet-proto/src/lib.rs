//! Wire types.
//!
//! `stage::v1` is a verbatim copy of the Ziren SDK's `stage.proto`, so the SDK's
//! network prover (`ZKM_PROVER=network`) talks to the scheduler unchanged.
//! `fleet::v1` is internal: scheduler <-> prover nodes.

pub mod artifacts;
pub mod sdk;

/// Shared file credential handling for endpoints; tokens never enter command arguments.
pub fn load_token(path: &std::path::Path) -> anyhow::Result<String> {
    let token = std::fs::read_to_string(path)?.trim().to_owned();
    anyhow::ensure!(token.len() >= 32, "token in {} is shorter than 32 characters", path.display());
    Ok(token)
}

pub mod stage {
    pub mod v1 {
        tonic::include_proto!("stage.v1");
    }
}

pub mod fleet {
    pub mod v1 {
        tonic::include_proto!("fleet.v1");
    }
}
