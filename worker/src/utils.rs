use std::{collections::HashMap, net::SocketAddr};
use tokio::{sync::Mutex};
use crate::worker_node_info::{PeerInfo};


pub async fn remove_peer(
    peers: &Mutex<HashMap<String, PeerInfo>>,
    addr: SocketAddr,
) {
    let mut map = peers.lock().await;
    map.remove(&addr.to_string());
}