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

    // build payload for signature
    let mut payload = node_id.as_bytes().to_vec();
    for peer in &peers {
        payload.extend_from_slice(peer.node_id.as_bytes());
        payload.extend_from_slice(&peer.pub_key);
    }

    let signature = signing_key
        .sign(&payload)
        .to_bytes()
        .to_vec();

    // send welcome to the new node
    let _ = tx
        .send(WireMessage::ServerToNode(
            ServerToNode::Welcome {
                supervisor_id: supervisor_id.to_string(),
                supervisor_pub_key: signing_key.verifying_key().to_bytes().to_vec(),
                peers,
                signature: signature.clone(),
            },
        ))
        .await;

    // broadcast NewPeer to all other nodes
    let map = workers.lock().await;
    for (id, w) in map.iter() {
        if id == &node_id {
            continue; // skip the new node
        }

        let _ = w.tx.send(WireMessage::ServerToNode(
            ServerToNode::NewPeer {
                node: PeerInfoMessage {
                    node_id: node_id.clone(),
                    addr,
                    pub_key: pub_key.clone(),
                    signature: signature.clone(),
                },
                signature: signature.clone(),
            },
        )).await;
    }
}