use std::collections::HashMap;
use std::net::SocketAddr;
use common::protocol::{NodeToServer, SupervisorEvent, WireMessage};
use tokio::{net::TcpStream, sync::mpsc, io::BufReader};

pub struct WorkerSession{
    tasks: HashMap<String, tokio::task::JoinHandle<()>>,
    pub worker_pub_key: Vec<u8>,
}

impl WorkerSession {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            worker_pub_key: Vec::new(),
        }
    }

     pub async fn run(
        mut self,
        socket: TcpStream,
        addr: SocketAddr,
        supervisor_tx: mpsc::Sender<SupervisorEvent>,
    ) {
        // Take ownership of halves immediately
        let (reader_half, writer_half) = socket.into_split();
        let mut reader = BufReader::new(reader_half);

        // Perform handshake
        let hello_node_to_server = match Self::handshake(&mut reader).await {
            Some(v) => v,
            None => return,
        };

        // Spawn writer task
        let (tx, writer_handle) = Self::spawn_writer(writer_half);
        self.tasks.insert("writer".to_string(), writer_handle);

        // Spawn reader task
        let reader_handle = Self::spawn_reader(reader, &supervisor_tx);
        self.tasks.insert("reader".to_string(), reader_handle);

        // Notify supervisor core of admission
        Self::emit_admit(&supervisor_tx, &hello_node_to_server, addr, &tx).await;

        // Wait for reader loop to finish
        if let Some(handle) = self.tasks.remove("reader") {
            let _ = handle.await;
        }

        // Remove worker
        if let NodeToServer::Hello { node_id, .. } = &hello_node_to_server {
            Self::emit_remove(&supervisor_tx, node_id.clone()).await;
        }

    }
}



impl WorkerSession {
    async fn emit_admit(
        supervisor_tx: &mpsc::Sender<SupervisorEvent>,
        hello: &NodeToServer,
        addr: SocketAddr,
        tx: &mpsc::Sender<WireMessage>,
   
    ) {
        if let NodeToServer::Hello { node_id, pub_key, .. } = hello {
        let _ = supervisor_tx.send(SupervisorEvent::Admit {
            node_id: node_id.clone(),
            addr,
            tx: tx.clone(),
            pub_key: pub_key.clone(),
        }).await;

        }
    }


}

impl WorkerSession {
    /// Signal the supervisor core to remove this worker
    async fn emit_remove(
        supervisor_tx: &mpsc::Sender<SupervisorEvent>,
        node_id: String,
    ) {
        let _ = supervisor_tx
            .send(SupervisorEvent::Remove { node_id })
            .await;
    }
}