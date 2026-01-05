use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{deserialize, protocol::{NodeToServer, WireMessage}};
use ed25519_dalek::{SigningKey, ed25519::signature::SignerMut};
use tokio::{io::{AsyncBufReadExt, BufReader}, net::tcp::OwnedReadHalf, time::sleep};

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
    tx: tokio::sync::mpsc::Sender<WireMessage>,
    node_id: String,
    mut signing_key: SigningKey,
    heartbeat_interval: Duration,
) {
    loop {
        println!("sending heartbeat ");
        sleep(heartbeat_interval).await;

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