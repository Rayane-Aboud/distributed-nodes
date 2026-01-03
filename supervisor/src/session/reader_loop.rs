
use tokio::{io::{AsyncBufReadExt, BufReader}, net::tcp::OwnedReadHalf, sync::mpsc, task::JoinHandle};
use common::{deserialize, protocol::{NodeToServer, SupervisorEvent, WireMessage}};

use crate::session::lifecycle::WorkerSession;
impl WorkerSession {
    pub fn spawn_reader(
    mut reader: BufReader<OwnedReadHalf>,
    supervisor_tx: &mpsc::Sender<SupervisorEvent>, // <- pass it here
) -> JoinHandle<()> {
        let tx = supervisor_tx.clone(); // clone for async move
        tokio::spawn(async move {
            let mut buffer = String::new();

            loop {
                let n = reader.read_line(&mut buffer).await.unwrap_or(0);
                if n == 0 {
                    break;
                }

                if let Some(msg) = deserialize::<WireMessage>(buffer.trim()) {
                    match msg {
                        WireMessage::NodeToServer(NodeToServer::Heartbeat { node_id, timestamp, .. }) => {
                            let _ = tx.send(SupervisorEvent::Heartbeat { node_id, timestamp }).await;
                        }


                        WireMessage::NodeToServer(NodeToServer::Disconnect { .. }) => {
                            break;
                        }

                        _ => {}
                    }
                }

                buffer.clear();
            }
        })
    }

}