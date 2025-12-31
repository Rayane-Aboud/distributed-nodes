use std::{collections::HashMap, net::SocketAddr};
use tokio::{sync::Mutex};
use crate::worker_node_info::{PeerInfo};


pub async fn handle_peer_session(
    //socket: TcpStream,
    //addr: SocketAddr,
    //peers: PeerRegistry
){
    //let (socket)
}

pub async fn register_peer(
    peers: &Mutex<HashMap<String, PeerInfo>>,
    node_id: String,
    addr: SocketAddr,
){
    let mut map = peers.lock().await;
    map.insert(
        addr.to_string(),
        PeerInfo { 
            node_id: node_id.clone(),
            addr: addr
        }
    );
}

pub async fn remove_peer(
    peers: &Mutex<HashMap<String, PeerInfo>>,
    addr: SocketAddr,
) {
    let mut map = peers.lock().await;
    map.remove(&addr.to_string());
}