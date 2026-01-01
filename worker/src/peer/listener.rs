
use common::protocol::{NodeToNode};
use tokio::net::TcpListener;
use tokio::sync::{mpsc};
use tokio::io::{AsyncWriteExt};

use common::{protocol::{ WireMessage}, serialize};

use crate::utils::register_peer;
use crate::worker_node_info::{PeerInfo, PeerRegistry};



pub async fn handle_peer_events(
    mut rx_peer_info: mpsc::Receiver<PeerInfo>,
    listener: TcpListener,
    peers: PeerRegistry,
){
    loop {
        tokio::select! {
            Some(peer) = rx_peer_info.recv() => {
                register_peer(&peers, peer.node_id, peer.addr).await
            }
            Ok((mut socket, remote_addr)) = listener.accept() => {
                let peer = {
                    let peers = peers.lock().await;
                    peers.get(&remote_addr.to_string()).cloned()
                };
                let Some(peer) = peer else {
                    drop(socket);
                    continue;
                };
                let msg = WireMessage::NodeToNode(
                    NodeToNode::JoinMessage {
                        id: peer.node_id.clone(),
                        addr: peer.addr,
                    }
                );

                let bytes = serialize(&msg);
                socket.write_all(bytes.as_bytes()).await.unwrap();
            }

        }
            
    }
}