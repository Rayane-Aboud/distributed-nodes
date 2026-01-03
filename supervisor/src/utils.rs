use std::{collections::HashMap, net::SocketAddr};

use common::{deserialize, protocol::{NodeToServer, WireMessage}};
use tokio::{io::{AsyncBufReadExt, BufReader}, sync::{Mutex, mpsc}, time::Instant};

use crate::worker_node_info::{WorkerInfo};

/* 
async fn insert_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    node_id: &str,
    addr: SocketAddr,
    tx: mpsc::Sender<WireMessage>,
) {
    let mut map = workers.lock().await;
    map.insert(
        addr.to_string(),
        WorkerInfo {
            addr,
            connected_at: Instant::now(),
            tx,
        },
    );
}*/

pub async fn remove_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    node_id: &str,
) {
    let mut map = workers.lock().await;
    map.remove(node_id);
}



pub async fn recv_worker_hello(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Option<common::protocol::NodeToServer> {
    let mut buffer = String::new();

    let n = reader.read_line(&mut buffer).await.ok()?; // return None if read fails
    if n == 0 {
        return None; // connection closed
    }

    if let Some(msg) = deserialize::<WireMessage>(buffer.trim()) {
        match msg {
            WireMessage::NodeToServer(NodeToServer::Hello { node_id, pub_key, signature }) => {
                Some(NodeToServer::Hello { node_id, pub_key, signature })
            }
            _ => None, // unexpected message
        }
    } else {
        None // deserialization failed
    }
}


