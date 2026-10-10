//! Wire types.
//!
//! `stage::v1` is a verbatim copy of the Ziren SDK's `stage.proto`, so the SDK's
//! network prover (`ZKM_PROVER=network`) talks to the scheduler unchanged.
//! `fleet::v1` is internal: scheduler <-> prover nodes.

pub mod artifacts;

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
