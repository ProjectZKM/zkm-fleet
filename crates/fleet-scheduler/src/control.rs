//! Prover registration and heartbeats (cluster-internal, cluster token required).

use std::sync::Arc;

use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::{
    control_service_server::ControlService, DeregisterRequest, DeregisterResponse,
    HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
};

use crate::fleet::Scheduler;

pub struct ProverControlService {
    pub fleet: Arc<Scheduler>,
}

#[tonic::async_trait]
impl ControlService for ProverControlService {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let node = request
            .into_inner()
            .node
            .ok_or_else(|| Status::invalid_argument("node missing"))?;
        let node_id = self
            .fleet
            .register(node)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        Ok(Response::new(RegisterResponse {
            node_id,
            heartbeat_secs: self.fleet.cfg.heartbeat_secs,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.fleet
            .exchange(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        self.fleet.deregister(&request.into_inner().node_id);
        Ok(Response::new(DeregisterResponse {}))
    }
}
