use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, mpsc}, time::Instant};

use crate::{tasks::{receive_from_supervisor, send_heartbeat_to_supervisor, send_hello_to_supervisor}, worker_node_info::{PeerInfo, PeerRegistry}};                    // Async TCP client

pub struct WorkerNode {
    id: String,
    supervisor: TcpStream,
    start: Instant,
    peers: PeerRegistry,
    peer_listener: TcpListener,
    listen_addr: SocketAddr,

    tx_peer_info: mpsc::Sender<PeerInfo>,
    rx_peer_info: mpsc::Receiver<PeerInfo>
}


impl WorkerNode {
    pub async fn new(id: String, supervisor_addr: &str, listen_addr:SocketAddr) -> Self {
        let supervisor = TcpStream::connect(supervisor_addr).await.unwrap();
        let start = Instant::now();
        let peers: Arc<Mutex<HashMap<String, PeerInfo>>> = Arc::new(Mutex::new(HashMap::new()));
        let peer_listener = TcpListener::bind(listen_addr).await.unwrap();

        let (tx_peer_info, rx_peer_info): (mpsc::Sender<PeerInfo>, mpsc::Receiver<PeerInfo>) = mpsc::channel(32);

        Self {id, supervisor,start, peers, peer_listener, listen_addr, tx_peer_info, rx_peer_info}
    }


    pub async fn run(self) {
        let (read, write) = self.supervisor.into_split();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // send hello once, synchronously, before loops
        {
            let mut write = write;
            send_hello_to_supervisor(&self.id, &mut write).await;

            let reader_task = tokio::spawn(
                receive_from_supervisor(read, self.tx_peer_info, shutdown_tx.clone())
            );

            let heartbeat_task = tokio::spawn(
                send_heartbeat_to_supervisor(self.id, write, self.start, shutdown_rx)
            );

            let _ = tokio::join!(reader_task, heartbeat_task);
        }
    }

}
