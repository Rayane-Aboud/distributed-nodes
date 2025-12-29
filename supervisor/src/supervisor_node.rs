use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::{TcpListener};
use tokio::sync::{Mutex, broadcast};

use crate::tasks::{run_cli, run_network};
use crate::worker_node_info::{ WorkerRegistry};




pub struct SupervisorNode {
    listener: TcpListener,
    tx: broadcast::Sender<String>,
    workers: WorkerRegistry,
}

impl SupervisorNode {
    pub async fn new(addr: &str) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (tx, _) = broadcast::channel(128);

        Self { 
            listener, 
            tx, 
            workers: Arc::new(Mutex::new(HashMap::new())), 
        }
    }

    pub async fn run(self) {
        
        // Network subsystem
        let net = tokio::spawn(run_network(
            self.listener,
            self.tx.clone(),
            self.workers.clone(),
        ));

        // CLI subsystem
        let cli = tokio::spawn(run_cli(self.workers.clone()));

        // Supervisor lives as long as both subsystems live
        let _ = tokio::join!(net, cli);
    }

}
