use std::time::Duration;
use tokio::io::AsyncWriteExt;
use common::{protocol::{NodeToServer, WireMessage}, serialize};
use tokio::{net::tcp::OwnedWriteHalf, time::Instant};



pub async fn send_heartbeat(
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
