use std::net::SocketAddr;

use common::protocol::{PeerInfoMessage, ServerToNode, WireMessage};
use ed25519_dalek::{SigningKey, ed25519::signature::{ SignerMut}};
use tokio::{sync::mpsc, time::Instant};
use crate::{worker_node_info::{WorkerInfo, WorkerRegistry}};



pub async fn admit_and_welcome(
    workers: &WorkerRegistry,
    mut signing_key: SigningKey,
    supervisor_id: &str,
    node_id: String,
    addr: SocketAddr,
    pub_key: Vec<u8>,
    tx: mpsc::Sender<WireMessage>,
) {
    // insert worker + snapshot peers atomically
    let peers: Vec<PeerInfoMessage> = {
        let mut map = workers.lock().await;

        map.insert(
            node_id.clone(),
            WorkerInfo {
                addr,
                connected_at: Instant::now(),
                pub_key: pub_key.clone(),
                tx: tx.clone(),
            },
        );

        map.iter()
            .filter(|(id, _)| *id != &node_id)
            .map(|(id, w)| PeerInfoMessage {
                node_id: id.clone(),
                addr: w.addr,
                pub_key: w.pub_key.clone(),
                signature: Vec::new(),
            })
            .collect()
    };

    // build payload
    let mut payload = node_id.as_bytes().to_vec();
    for peer in &peers {
        payload.extend_from_slice(peer.node_id.as_bytes());
        payload.extend_from_slice(&peer.pub_key);
    }

    // sign
    let signature = signing_key
        .sign(&payload)
        .to_bytes()
        .to_vec();

    // send welcome
    let _ = tx
        .send(WireMessage::ServerToNode(
            ServerToNode::Welcome {
                supervisor_id: supervisor_id.to_string(),
                supervisor_pub_key: signing_key
                    .verifying_key()
                    .to_bytes()
                    .to_vec(),
                peers,
                signature,
            },
        ))
        .await;
}