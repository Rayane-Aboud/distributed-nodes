use std::time::Duration;

use common::deserialize;
use common::protocol::{NodeToNode, ServerToNode};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{mpsc, watch};
use tokio::{net::tcp::OwnedWriteHalf, time::Instant};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::io::AsyncBufReadExt;

use common::{protocol::{NodeToServer, WireMessage}, serialize};

use crate::utils::register_peer;
use crate::worker_node_info::{PeerInfo, PeerRegistry};

pub async fn send_hello_to_supervisor(id: &str, write: &mut OwnedWriteHalf) {
    let hello = WireMessage::NodeToServer(NodeToServer::Hello { node_id: id.to_string() });

    write
        .write_all(format!("{}\n", serialize(hello)).as_bytes())
        .await
        .unwrap();
}

pub async fn send_heartbeat_to_supervisor(
    id: String,
    mut write: OwnedWriteHalf,
    start: Instant,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                break;
            }

            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let heartbeat = WireMessage::NodeToServer(
                    NodeToServer::Heartbeat {
                        node_id: id.clone(),
                        timestamp: start.elapsed().as_millis() as u64,
                    }
                );

                if write
                    .write_all(format!("{}\n", serialize(heartbeat)).as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}



pub async fn receive_from_supervisor(
    read: OwnedReadHalf,
    tx_peer_info: mpsc::Sender<PeerInfo>,
    shutdown: watch::Sender<bool>,
) {    
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();

    loop {
        match reader.read_line(&mut buffer).await.unwrap_or(0) {
            0 => break,
            _ => {
                let msg: WireMessage = deserialize(buffer.trim());
                match msg {
                    WireMessage::ServerToNode(ServerToNode::Welcome { .. }) => {}
                    WireMessage::ServerToNode(ServerToNode::Shutdown { .. }) => break,
                    WireMessage::ServerToNode(ServerToNode::NewPeer { node }) => {
                        let _ = tx_peer_info.send(PeerInfo {
                            node_id: node.node_id,
                            addr: node.addr,
                        }).await;
                    }
                    _ => break,
                }
                buffer.clear();
            }
        }
    }

    let _ = shutdown.send(true);

}




pub async fn peer_connection(
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