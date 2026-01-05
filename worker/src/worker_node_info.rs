use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{sync::{Mutex, mpsc}, };

use common::protocol::{PeerInfoMessage, WireMessage};

#[derive(Clone)]
pub struct PeerInfo {
    pub node_id: String,
    pub addr: SocketAddr,
    pub pub_key: Vec<u8>,       
    pub signature: Option<Vec<u8>>,
    pub tx: Option<mpsc::Sender<WireMessage>>
}

impl PeerInfo {
    pub fn new(msg: PeerInfoMessage) -> Self {
        Self {
            node_id: msg.node_id,
            addr: msg.addr,
            pub_key: msg.pub_key,
            signature: Some(msg.signature),
            tx: None,
        }
    }
}





pub type PeerRegistry = Arc<Mutex<HashMap<String, PeerInfo>>>;