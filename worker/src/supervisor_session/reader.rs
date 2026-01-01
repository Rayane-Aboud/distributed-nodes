use common::deserialize;
use common::protocol::{ServerToNode, WireMessage};
use tokio::{io::BufReader, net::tcp::OwnedReadHalf, sync::mpsc};
use tokio::io::AsyncBufReadExt;


use crate::worker_node_info::PeerInfo;

pub async fn receive_messages(
    read: OwnedReadHalf,
    tx_peer_info: mpsc::Sender<PeerInfo>,
    shutdown: tokio::sync::watch::Sender<bool>,
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
