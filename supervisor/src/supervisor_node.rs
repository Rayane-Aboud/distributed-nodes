use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::{TcpListener};
use tokio::sync::{Mutex};

use crate::tasks::{run_cli, accept_node_connections};
use crate::worker_node_info::{ WorkerRegistry};




pub struct SupervisorNode {
    listener: TcpListener,
    workers: WorkerRegistry,
}

impl SupervisorNode {
    pub async fn new(addr: &str) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();

        Self { 
            listener, 
            workers: Arc::new(Mutex::new(HashMap::new())), 
        }
    }

    pub async fn run(self) {
        
        // Network subsystem
        let net = tokio::spawn(accept_node_connections(
            self.listener,
            self.workers.clone(),
        ));

        // CLI subsystem
        let cli = tokio::spawn(run_cli(self.workers.clone()));

        // Supervisor lives as long as both subsystems live
        let _ = tokio::join!(net, cli);
    }

}
