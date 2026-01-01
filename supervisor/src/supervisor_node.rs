use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::{TcpListener};
use tokio::sync::{Mutex, mpsc};

use crate::session::run_worker_session;
use crate::supervisor_core::run_supervisor_core;
use crate::tasks::{run_cli, handle_worker_nodes};
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
        
        let (
            //session thread uses this to send events to the core
            tx_supervisor,
             
            //supervisor core uses this to receive from sessions
            rx_supervisor
        
        ) = mpsc::channel(128);
        
        tokio::spawn(run_supervisor_core(self.workers.clone(), rx_supervisor));

        loop {
            let (socket, addr) = self.listener.accept().await.unwrap();
            tokio::spawn(run_worker_session(socket,addr, tx_supervisor.clone()));
        }
    }

}
