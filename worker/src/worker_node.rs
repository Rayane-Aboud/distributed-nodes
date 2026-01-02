use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, mpsc}, time::Instant};

use crate::{peer, supervisor_session, worker_node_info::{PeerInfo, PeerRegistry}};


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

        let (supervisor_read, supervisor_write) = self.supervisor.into_split();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let hello_task = tokio::spawn(supervisor_session::hello::send_hello(
            &self.id,
            &mut supervisor_write
        ));

        let reader_task = tokio::spawn(supervisor_session::reader::receive_messages(
            supervisor_read,
            self.tx_peer_info.clone(),
            shutdown_tx.clone()
        ));

        let heartbeat_task = tokio::spawn(supervisor_session::heartbeat::send_heartbeat(
            self.id.clone(),
            supervisor_write,
            self.start,
            shutdown_rx
        ));

        let peer_listener_task = tokio::spawn(peer::listener::handle_peer_events(
            self.rx_peer_info,
            self.peer_listener,
            self.peers.clone()
        ));


        let _ = tokio::join!(
            hello_task,
            reader_task,
            heartbeat_task,
            peer_listener_task
        );


    }

}
