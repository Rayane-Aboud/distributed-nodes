use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use common::protocol::WireMessage;
use tokio::{sync::{Mutex, mpsc}, time::Instant};


pub type WorkerRegistry = Arc<Mutex<HashMap<String, WorkerInfo>>>;


#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub addr: SocketAddr,
    pub connected_at: Instant,
    pub tx: mpsc::Sender<WireMessage>
}