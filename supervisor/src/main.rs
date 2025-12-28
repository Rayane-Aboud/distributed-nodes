mod worker_node;
mod tasks;
mod utils;
mod supervisor_node;


use crate::supervisor_node::SupervisorNode;




#[tokio::main]
async fn main() {
    let supervisor = SupervisorNode::new("127.0.0.1:9000").await;
    supervisor.run().await;
}
