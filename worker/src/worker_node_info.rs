use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use common::protocol::PeerInfoMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: String,
    pub addr: SocketAddr,
    pub pub_key: Vec<u8>,       
    pub signature: Vec<u8>,
}

impl From<PeerInfoMessage> for PeerInfo {
    fn from(msg: PeerInfoMessage) -> Self {
        PeerInfo {
            node_id: msg.node_id,
            addr: msg.addr,
            pub_key: msg.pub_key,
            signature: msg.signature,
        }
    }
}


pub type PeerRegistry = Arc<Mutex<HashMap<String, PeerInfo>>>;