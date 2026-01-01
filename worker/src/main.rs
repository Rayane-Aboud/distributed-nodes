use std::net::SocketAddr;

use crate::worker_node::WorkerNode;

mod tasks;
mod worker_node_info;
mod utils;
mod worker_node;
mod peer;
mod protocol;
mod supervisor_session;


#[tokio::main]
async fn main() {
    let id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "worker".to_string());
    let listen_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let worker = WorkerNode::new(id, "127.0.0.1:9000", listen_addr).await;


    worker.run().await;
}
