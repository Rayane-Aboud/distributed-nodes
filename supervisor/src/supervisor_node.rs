use std::collections::HashMap;
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use rand_core::OsRng;

use tokio::net::{TcpListener};
use tokio::sync::{Mutex, mpsc};

use crate::session::lifecycle::{WorkerSession};

use crate::worker_node_info::{ WorkerRegistry};


pub struct SupervisorNode {
    pub id: String,
    pub listener: TcpListener,
    pub workers: WorkerRegistry,
    pub signing_key: SigningKey
}

impl SupervisorNode {
    pub async fn new(addr: &str) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        let mut csprng = OsRng {};
        let signing_key = SigningKey::generate(&mut csprng);

        Self { 
            id: "sup-1".into(),
            listener,
            workers: Arc::new(Mutex::new(HashMap::new())),
            signing_key,
        }


    }

    pub async fn run(self) {
        // session → supervisor core channel
        let (tx_supervisor, rx_supervisor) = mpsc::channel(128);

        // extract core-owned state
        let workers = self.workers.clone();
        let signing_key = self.signing_key.clone();
        let supervisor_id = self.id.clone();

        // spawn supervisor core
        tokio::spawn(async move {
            SupervisorNode::run_supervisor_core(
                workers,
                signing_key,
                supervisor_id,
                rx_supervisor,
            )
            .await;
        });

        // accept loop stays in this task
        let listener = self.listener;

        loop {
            let (socket, addr) = listener.accept().await.unwrap();
            tokio::spawn(
                WorkerSession::new().run(socket, addr, tx_supervisor.clone())
            );
        }
    }

}
