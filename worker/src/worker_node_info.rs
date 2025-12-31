use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: String,
    pub addr: SocketAddr,
}

pub type PeerRegistry = Arc<Mutex<HashMap<String, PeerInfo>>>;