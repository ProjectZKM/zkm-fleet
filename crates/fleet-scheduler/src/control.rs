//! Node registration and heartbeats (cluster-internal, cluster token required).

use std::sync::Arc;

use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::{
    control_service_server::ControlService, DeregisterRequest, DeregisterResponse,
    FetchInputsRequest, HeartbeatRequest, HeartbeatResponse, ProveRequest, RegisterRequest,
    RegisterResponse,
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
        self.fleet
            .exchange(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn fetch_inputs(
        &self,
        request: Request<FetchInputsRequest>,
    ) -> Result<Response<ProveRequest>, Status> {
        let r = request.into_inner();
        let assignment = r
            .assignment
            .ok_or_else(|| Status::invalid_argument("assignment missing"))?;
        self.fleet
            .fetch_inputs(&r.node_id, &assignment, r.include_elf)
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
