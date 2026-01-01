use std::net::SocketAddr;
use common::protocol::WireMessage;
use tokio::{net::TcpStream, sync::mpsc, io::BufReader};

use crate::session::{session_read_loop, spawn_writer};
use crate::supervisor_core::SupervisorEvent;
use crate::session::{handshake::handshake};

pub async fn run_worker_session(
    socket: TcpStream,
    addr: SocketAddr,
    supervisor_tx: mpsc::Sender<SupervisorEvent>,
) {
    let (mut reader, writer) = split_socket(socket);

    let node_id = match handshake(&mut reader).await {
        Some(id) => id,
        None => return,
    };

    let tx = spawn_writer(writer);

    admit_worker(&supervisor_tx, &node_id, addr, &tx).await;

    session_read_loop(&mut reader).await;

    supervisor_tx.send(SupervisorEvent::Remove { node_id }).await.ok();
}

fn split_socket(
    socket: TcpStream,
) -> (
    BufReader<tokio::net::tcp::OwnedReadHalf>,
    tokio::net::tcp::OwnedWriteHalf,
) {
    let (read, write) = socket.into_split();
    (BufReader::new(read), write)
}

async fn admit_worker(
    supervisor_tx: &mpsc::Sender<SupervisorEvent>,
    node_id: &str,
    addr: SocketAddr,
    tx: &mpsc::Sender<WireMessage>,
) {
    use common::protocol::{WireMessage, ServerToNode, PeerInfoMessage};

    supervisor_tx.send(SupervisorEvent::Admit {
        node_id: node_id.to_string(),
        addr,
        tx: tx.clone(),
    }).await.ok();

    supervisor_tx.send(SupervisorEvent::SendTo {
        node_id: node_id.to_string(),
        msg: WireMessage::ServerToNode(ServerToNode::Welcome {
            supervisor_id: "supervisor-1".into(),
        }),
    }).await.ok();

    supervisor_tx.send(SupervisorEvent::Broadcast {
        msg: WireMessage::ServerToNode(ServerToNode::NewPeer {
            node: PeerInfoMessage {
                node_id: node_id.to_string(),
                addr,
            },
        }),
        except: Some(node_id.to_string()),
    }).await.ok();
}
