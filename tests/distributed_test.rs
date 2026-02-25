use miracledb::cluster::{start_cluster_node, proto};
use tonic::transport::Channel;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_distributed_node_registration() {
    // Start server
    let addr = "127.0.0.1:50051".parse().unwrap();
    
    tokio::spawn(async move {
        start_cluster_node(addr).await.unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_millis(500)).await;
    
    // Connect client
    let channel = Channel::from_static("http://127.0.0.1:50051")
        .connect()
        .await
        .unwrap();
        
    let mut client = proto::cluster_service_client::ClusterServiceClient::new(channel);
    
    let request = tonic::Request::new(proto::RegisterNodeRequest {
        node_id: "test-node-1".to_string(),
        address: "127.0.0.1".to_string(),
        port: 50051,
    });
    
    let response = client.register_node(request).await.unwrap();
    assert!(response.into_inner().success);
}
