use std::net::SocketAddr;

use tokio::{net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{mpsc::{self, Sender}, oneshot}, task::JoinHandle};
use common::{protocol::{NodeToNode, WireMessage, WorkerEvent}};
use tokio::io::{BufReader, AsyncWriteExt};

use crate::{utils::read_wire_message, worker_node_info::{PeerInfo}};

pub struct PeerSession;

impl PeerSession {
    pub async fn run_initiator(tx_core: Sender<WorkerEvent>, addr: SocketAddr,peer: PeerInfo) {
        let stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(_) => return,
        };

        let (read_half, write_half) = stream.into_split();
        let (tx, _writer_handle) = Self::spawn_writer_to_peer(write_half);

        // Send PeerHello immediately
        let msg = WireMessage::NodeToNode(
            NodeToNode::PeerHello {
                node_id: peer.node_id.clone(),
                pub_key: peer.pub_key.clone(),
                signature: None,
            }
        );

        let _ = tx.send(msg).await;

        Self::peer_reader_loop(tx_core.clone() , read_half).await;
    }

    pub async fn run_acceptor(
        socket: TcpStream,
        tx_core: Sender<WorkerEvent>,
        addr: SocketAddr,
    ) {
        let (read_half, write_half) = socket.into_split();
        let mut reader = BufReader::new(read_half);

        let msg = match read_wire_message(&mut reader).await {
            Some(m) => m,
            None => return,
        };

        match msg {
            WireMessage::NodeToNode(NodeToNode::PeerHello {
                node_id,
                pub_key,
                signature,
            }) => {
                let (tx, _writer_handle) = Self::spawn_writer_to_peer(write_half);

                let (admit_tx, admit_rx) = oneshot::channel();

                // notify core, worker doesn't mutate the state of the nodes
                let _ = tx_core
                    .send(WorkerEvent::InboundPeerHello {
                        node_id,
                        addr,
                        pub_key,
                        signature,
                        tx: tx.clone(),
                        admit_tx,
                    })
                    .await;

                // wait for core decision
                if admit_rx.await.is_err() {
                    return;
                }

                // only after admission
                //start listening
                Self::peer_reader_loop(tx_core, reader.into_inner()).await;
            }

            _ => return,
        }
    }


    fn spawn_writer_to_peer(
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

    async fn peer_reader_loop(
        tx_core: Sender<WorkerEvent>,
        read_half: OwnedReadHalf,
    ) {
        let mut reader = BufReader::new(read_half);

        while let Some(msg) = read_wire_message(&mut reader).await {
            match msg {
                WireMessage::NodeToNode(_) => {
                    //needs better handling
                    let _ = tx_core.send(WorkerEvent::MessageFromPeer { message: "received from peer".to_string() });
                }
                _ => {}
            }
        }
    }
}