use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub addr: SocketAddr,
}

pub type WorkerRegistry = Arc<Mutex<HashMap<String, WorkerInfo>>>;
