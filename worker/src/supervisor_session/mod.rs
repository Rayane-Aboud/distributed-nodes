use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{deserialize, protocol::{NodeToServer, ServerToNode, WireMessage, WorkerEvent}};
use ed25519_dalek::{SigningKey, ed25519::signature::SignerMut};
use tokio::{io::BufReader, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::mpsc, task::JoinHandle, time::sleep};
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncBufReadExt;

pub struct SupervisorSession {
    /// outbound messages to supervisor
    tx: mpsc::Sender<WireMessage>,
}

impl SupervisorSession {
    pub async fn run(
        socket: TcpStream,
        node_id: String,
        signing_key: SigningKey,
        worker_tx: mpsc::Sender<WorkerEvent>,
    ) {
        // 1. Split the TCP stream.
        //    From now on:
        //    - reader owns read_half
        //    - writer owns write_half
        let (read_half, write_half) = socket.into_split();

        // 2. Spawn the writer task.
        //    This gives us an mpsc::Sender<WireMessage>.
        //    ANY task can now send messages to the supervisor
        //    without touching the socket directly.
        let (tx, writer_handle) = spawn_writer(write_half);

        // 3. Immediately send Hello.
        //    This is REQUIRED by the supervisor handshake.
        //    Nothing else may be sent before this.
        send_hello(&tx, &node_id, signing_key.clone()).await;

        // 4. Prepare the reader.
        //    The reader will now receive ANY message
        //    the supervisor decides to send.
        let mut reader = BufReader::new(read_half);

        // 5. HANDSHAKE PHASE (worker-side only)
        //
        //    At this point:
        //    - Supervisor may already have processed Admit
        //    - Supervisor may already have enqueued Welcome
        //
        //    The worker MUST ignore everything until it sees Welcome.
        let welcome = loop {
            let msg = match read_wire_message(&mut reader).await {
                Some(m) => m,
                None => {
                    // Socket closed before Welcome → handshake failed
                    return;
                }
            };

            match msg {
                WireMessage::ServerToNode(ServerToNode::Welcome { .. }) => {
                    // This is the ONLY message that transitions
                    // the worker into an admitted state.
                    break msg;
                }

                // Any other message before Welcome is a protocol violation.
                // We terminate immediately.
                _ => {
                    return;
                }
            }
        };

        // 6. We are now ADMITTED.
        //    Notify WorkerNode so it can:
        //    - populate peer registry
        //    - start outbound peer connections
        if let WireMessage::ServerToNode(ServerToNode::Welcome {
            supervisor_id,
            peers,
            ..
        }) = welcome {
            let _ = worker_tx.send(
                WorkerEvent::SupervisorWelcome {
                    supervisor_id,
                    peers,
                }
            ).await;
        }

        // 7. Start heartbeat ONLY AFTER admission.
        //    Before Welcome, heartbeats are illegal.
        let hb_tx = tx.clone();
        let hb_node_id = node_id.clone();
        let hb_key = signing_key.clone();

        tokio::spawn(async move {
            heartbeat_loop(hb_tx, hb_node_id, hb_key).await;
            
        });

        // 8. ACTIVE SESSION PHASE
        //
        //    From now on, we process normal supervisor messages:
        //    - NewPeer
        //    - Command
        //    - Shutdown
        //
        //    Any other message is ignored or treated as error.
        loop {
            let msg = match read_wire_message(&mut reader).await {
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
                    // Ignore anything else.
                }
            }
        }

        // 9. Best-effort Disconnect.
        //    Supervisor already knows or will learn soon.
        let _ = tx.send(
            WireMessage::NodeToServer(
                NodeToServer::Disconnect {
                    reason: "worker session ended".into(),
                    signature: None,
                }
            )
        ).await;

        // 10. Wait for writer to flush and exit.
        let _ = writer_handle.await;
    }
}


pub fn spawn_writer(
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

pub async fn send_hello(
    tx: &mpsc::Sender<WireMessage>,
    node_id: &str,
    mut signing_key: SigningKey,
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


pub async fn read_wire_message(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Option<WireMessage> {
    let mut line = String::new();

    let n = reader.read_line(&mut line).await.ok()?;
    if n == 0 {
        return None; // socket closed
    }

    // deserialize returns Option<WireMessage>
    deserialize::<WireMessage>(line.trim())
}

pub async fn heartbeat_loop(
    tx: mpsc::Sender<WireMessage>,
    node_id: String,
    mut signing_key: SigningKey,
) {
    loop {
        println!("sending heartbeat ");
        sleep(Duration::from_secs(5)).await;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut payload = node_id.as_bytes().to_vec();
        payload.extend_from_slice(&timestamp.to_be_bytes());

        let signature = signing_key
            .sign(&payload)
            .to_bytes()
            .to_vec();

        let msg = WireMessage::NodeToServer(
            NodeToServer::Heartbeat {
                node_id: node_id.clone(),
                timestamp,
                signature: Some(signature),
            }
        );

        if tx.send(msg).await.is_err() {
            break;
        }

        
    }
}
