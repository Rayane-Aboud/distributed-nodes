mod worker_node_info;
mod tasks;
mod utils;
mod supervisor_node;
mod protocol;
mod session;
mod supervisor_core;


use crate::supervisor_node::SupervisorNode;

#[tokio::main]
async fn main() {
    let supervisor = SupervisorNode::new("127.0.0.1:9000").await;
    supervisor.run().await;
}
