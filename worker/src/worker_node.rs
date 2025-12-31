use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{net::{TcpListener, TcpStream}, sync::Mutex, time::Instant};

use crate::{tasks::{receive_from_supervisor, send_heartbeat_to_supervisor, send_hello_to_supervisor}, worker_node_info::{PeerInfo, PeerRegistry}};                    // Async TCP client

pub struct WorkerNode {
    id: String,
    supervisor: TcpStream,
    start: Instant,
    peers: PeerRegistry,
    peer_listener: TcpListener,
    listen_addr: SocketAddr
}


impl WorkerNode {
    pub async fn new(id: String, supervisor_addr: &str, listen_addr:SocketAddr) -> Self {
        let supervisor = TcpStream::connect(supervisor_addr).await.unwrap();
        let start = Instant::now();
        let peers: Arc<Mutex<HashMap<String, PeerInfo>>> = Arc::new(Mutex::new(HashMap::new()));
        let peer_listener = TcpListener::bind(listen_addr).await.unwrap();
        Self {id, supervisor,start, peers, peer_listener, listen_addr}
    }


    pub async fn run(self) {
        let (read,mut write) = self.supervisor.into_split();
        //send hello first
        send_hello_to_supervisor(&self.id, &mut write).await;


        //start the loops

        let supervisor_receiver = tokio::spawn(receive_from_supervisor(read));
        let start = self.start;
        let hb_sender = tokio::spawn(send_heartbeat_to_supervisor(self.id, write, start));

        

        let _ = tokio::join!(supervisor_receiver, hb_sender);
    }
}
