use std::net::SocketAddr;

use crate::worker_node::WorkerNode;

mod worker_node_info;
mod utils;
mod worker_node;
mod supervisor_session;
mod worker_session;


#[tokio::main]
async fn main() {
    let node = WorkerNode::new(
        "worker-1".to_string(),
        "127.0.0.1:9000",
        "127.0.0.1:9101".parse::<SocketAddr>().unwrap(),
    )
    .await;

    node.run().await;
}