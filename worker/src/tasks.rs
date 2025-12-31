use std::time::Duration;

use common::deserialize;
use common::protocol::ServerToNode;
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedReadHalf;
use tokio::{net::tcp::OwnedWriteHalf, time::Instant};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::io::AsyncBufReadExt;

use common::{protocol::{NodeToServer, WireMessage}, serialize};

use crate::worker_node_info::PeerRegistry;

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
    start: Instant
) {

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

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


pub async fn receive_from_supervisor(read: OwnedReadHalf) {
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();

    loop {
        match reader.read_line(&mut buffer).await.unwrap_or(0){
            0 => break,
            _ => {
                let msg: WireMessage = deserialize(buffer.trim());
                match msg {
                    WireMessage::ServerToNode(ServerToNode::Welcome { .. }) => {
                        // mark connected (log only)
                        println!("welcomed by the supervisor");
                    }
                    WireMessage::ServerToNode(ServerToNode::Shutdown { .. }) => {
                        break;
                    }
                    _ => {
                        break; // protocol violation
                    }
                }
                buffer.clear();
            }
        }
    }
}




pub async fn accept_peer_connection(
    id: String,
    listener: TcpListener,
    peers: PeerRegistry,
){
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let peers = peers.clone();
        let id = id.clone();

        tokio::spawn(handle_peer_session(socket, peers));
    }
}