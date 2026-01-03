use tokio::{net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::mpsc, task::JoinHandle};
use common::protocol::{NodeToNode, PeerInfoMessage, WireMessage};
use tokio::io::BufReader;
use tokio::io::AsyncWriteExt;


use crate::{supervisor_session::read_wire_message, worker_node_info::PeerRegistry};

pub struct PeerSession;

impl PeerSession {
    pub async fn run_outbound(peer: PeerInfoMessage) {
        let stream = match TcpStream::connect(peer.addr).await {
            Ok(s) => s,
            Err(_) => return,
        };

        let (read_half, write_half) = stream.into_split();

        let (tx, _writer_handle) = spawn_peer_writer(write_half);

        // Send PeerHello immediately
        let msg = WireMessage::NodeToNode(
            NodeToNode::PeerHello {
                node_id: peer.node_id.clone(),
                signature: None,
            }
        );

        let _ = tx.send(msg).await;

        peer_reader_loop(read_half).await;
    }
}


impl PeerSession {
    pub async fn run_inbound(
        socket: TcpStream,
        _addr: std::net::SocketAddr,
        _peers: PeerRegistry,
    ) {
        let (read_half, write_half) = socket.into_split();

        let mut reader = BufReader::new(read_half);

        // Expect PeerHello first
        let msg = match read_wire_message(&mut reader).await {
            Some(m) => m,
            None => return,
        };

        match msg {
            WireMessage::NodeToNode(NodeToNode::PeerHello { .. }) => {
                // Accept peer for now (no auth yet)
                let (_tx, _writer_handle) = spawn_peer_writer(write_half);
                peer_reader_loop(reader.into_inner()).await;
            }
            _ => {
                // Protocol violation
                return;
            }
        }
    }
}


pub fn spawn_peer_writer(
    mut writer: OwnedWriteHalf,
) -> (mpsc::Sender<WireMessage>, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel(32);

    let handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let _ = writer
                .write_all(format!("{}\n", common::serialize(msg)).as_bytes())
                .await;
        }
    });

    (tx, handle)
}


pub async fn peer_reader_loop(
    read_half: OwnedReadHalf,
) {
    let mut reader = BufReader::new(read_half);

    while let Some(msg) = read_wire_message(&mut reader).await {
        match msg {
            WireMessage::NodeToNode(_) => {
                // handle later
            }
            _ => {}
        }
    }
}
