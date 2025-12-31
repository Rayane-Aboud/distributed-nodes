mod tasks;
mod worker_node_info;
mod utils;
mod worker_node;



#[tokio::main]
async fn main() {
    // Resolve worker identity from CLI
    let id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "worker".to_string());

    // Construct node
    //let worker = WorkerNode::new(id, "127.0.0.1:9000").await;

    // Run node lifecycle
    worker.run().await;
}
