use tonic::{transport::Server, Request, Response, Status};
// Include generated proto code
pub mod proto {
    tonic::include_proto!("cluster");
}
pub mod sharding;
pub mod node;
pub mod registry;
pub mod coordinator;

pub use node::{ClusterNode, NodeId, NodeStatus};
pub use registry::NodeRegistry;
pub use coordinator::QueryCoordinator;

use proto::cluster_service_server::{ClusterService, ClusterServiceServer};
use proto::{RegisterNodeRequest, RegisterNodeResponse, HeartbeatRequest, HeartbeatResponse, SubmitTaskRequest, SubmitTaskResponse};

pub struct MyClusterService;

#[tonic::async_trait]
impl ClusterService for MyClusterService {
    async fn register_node(&self, request: Request<RegisterNodeRequest>) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        // logic here
        Ok(Response::new(RegisterNodeResponse {
            success: true,
            message: format!("Node {} registered", req.node_id),
        }))
    }

    async fn heartbeat(&self, _request: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            limit_exceeded: false,
        }))
    }

    async fn submit_task(&self, _request: Request<SubmitTaskRequest>) -> Result<Response<SubmitTaskResponse>, Status> {
        Ok(Response::new(SubmitTaskResponse {
            job_id: "job-123".to_string(),
            accepted: true,
        }))
    }
}

pub async fn start_cluster_node(addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let service = MyClusterService;
    Server::builder()
        .add_service(ClusterServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}
