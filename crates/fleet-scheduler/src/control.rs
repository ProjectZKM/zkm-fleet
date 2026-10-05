//! Node registration and heartbeats (cluster-internal, cluster token required).

use std::sync::Arc;

use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::{
    control_service_server::ControlService, DeregisterRequest, DeregisterResponse,
    HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
};

use crate::fleet::Fleet;

pub struct ControlApi {
    pub fleet: Arc<Fleet>,
}

#[tonic::async_trait]
impl ControlService for ControlApi {
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
        let r = request.into_inner();
        Ok(Response::new(HeartbeatResponse {
            known: self.fleet.heartbeat(
                &r.node_id,
                r.ready,
                r.cached_elf_ids,
                r.busy,
                &r.running_job,
            ),
        }))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        self.fleet.deregister(&request.into_inner().node_id);
        Ok(Response::new(DeregisterResponse {}))
    }
}
