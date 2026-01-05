use common::protocol::{NodeToServer, ServerToNode, WireMessage, WorkerEvent};
use ed25519_dalek::{SigningKey, ed25519::signature::SignerMut};
use tokio::{io::BufReader, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::mpsc, task::JoinHandle};
use tokio::io::AsyncWriteExt;

use crate::utils::{read_wire_message, heartbeat_loop};

pub struct SupervisorSession;

impl SupervisorSession {
    pub async fn run(
        socket: TcpStream,
        node_id: String,
        signing_key: SigningKey,
        worker_tx: mpsc::Sender<WorkerEvent>,
    ) {
        let (read_half, write_half) = socket.into_split();
        let (tx, 
            writer_handle) = Self::spawn_writer(write_half);
        let mut reader = BufReader::new(read_half);
        
        // 1. Initial handshake
        if !Self::perform_handshake(&tx, &mut reader, &node_id, signing_key.clone(), &worker_tx).await {
            return;
        }
        
        // 2. Start heartbeat
        let hb_tx = tx.clone();
        let hb_node_id = node_id.clone();
        let hb_key = signing_key.clone();
        tokio::spawn(async move {
            heartbeat_loop(hb_tx, hb_node_id, hb_key, std::time::Duration::from_secs(5)).await;
        });
        
        // 3. Process session messages (using the same reader)
        Self::process_session_messages(&mut reader, &tx, worker_tx).await;
        
        // 4. Clean shutdown
        Self::send_disconnect(&tx).await;
        let _ = writer_handle.await;
    }

    fn spawn_writer(
        mut writer: OwnedWriteHalf,
    ) -> (mpsc::Sender<WireMessage>, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<WireMessage>(32);

        let handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let serialized = common::serialize(msg);
                let _ = writer
                    .write_all(format!("{}\n", serialized).as_bytes())
                    .await;
            }
        });

        (tx, handle)
    }

    async fn perform_handshake(
        tx: &mpsc::Sender<WireMessage>,
        reader: &mut BufReader<OwnedReadHalf>,
        node_id: &str,
        mut signing_key: SigningKey,
        worker_tx: &mpsc::Sender<WorkerEvent>,
    ) -> bool {
        // Send Hello message
        Self::send_hello(tx, node_id, &mut signing_key).await;
        
        // Wait for Welcome message
        match Self::wait_for_welcome(reader).await {
            Some(WireMessage::ServerToNode(ServerToNode::Welcome {
                supervisor_id,
                peers,
                ..
            })) => {
                // Notify worker about successful handshake
                let _ = worker_tx.send(
                    WorkerEvent::SupervisorWelcome {
                        supervisor_id,
                        peers,
                    }
                ).await;
                true
            }
            _ => {
                // Socket closed or invalid message before Welcome
                false
            }
        }
    }

    async fn wait_for_welcome(
        reader: &mut BufReader<OwnedReadHalf>,
    ) -> Option<WireMessage> {
        loop {
            let msg = read_wire_message(reader).await?;
            
            match msg {
                WireMessage::ServerToNode(ServerToNode::Welcome { .. }) => {
                    return Some(msg);
                }
                _ => {
                    // Any other message before Welcome is a protocol violation
                    return None;
                }
            }
        }
    }

    async fn process_session_messages(
        reader: &mut BufReader<OwnedReadHalf>,
        tx: &mpsc::Sender<WireMessage>,
        worker_tx: mpsc::Sender<WorkerEvent>,
    ) {
        loop {
            let msg = match read_wire_message(reader).await {
                Some(m) => m,
                None => break, // socket closed
            };

            match msg {
                WireMessage::ServerToNode(ServerToNode::NewPeer { node, .. }) => {
                    println!("new peer arrived");
                    let _ = worker_tx.send(
                        WorkerEvent::NewPeer { peer: node }
                    ).await;
                }

                WireMessage::ServerToNode(ServerToNode::Shutdown { reason, .. }) => {
                    let _ = worker_tx.send(
                        WorkerEvent::SupervisorShutdown { reason }
                    ).await;
                    break;
                }

                _ => {
                    // Ignore anything else
                }
            }
        }
    }

    async fn send_disconnect(tx: &mpsc::Sender<WireMessage>) {
        let _ = tx.send(
            WireMessage::NodeToServer(
                NodeToServer::Disconnect {
                    reason: "worker session ended".into(),
                    signature: None,
                }
            )
        ).await;
    }

    async fn send_hello(
        tx: &mpsc::Sender<WireMessage>,
        node_id: &str,
        signing_key: &mut SigningKey,
    ) {
        let signature = signing_key
            .sign(node_id.as_bytes())
            .to_bytes()
            .to_vec();

        let msg = WireMessage::NodeToServer(
            NodeToServer::Hello {
                node_id: node_id.to_string(),
                pub_key: signing_key
                    .verifying_key()
                    .to_bytes()
                    .to_vec(),
                signature: Some(signature),
            }
        );

        let _ = tx.send(msg).await;
    }
}