use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;

pub struct PeerInfo {
    pub node_id: String,
    pub addr: SocketAddr,
}

pub type PeerRegistry = Arc<Mutex<HashMap<String, PeerInfo>>>;