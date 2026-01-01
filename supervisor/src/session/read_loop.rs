use tokio::io::{BufReader, AsyncBufReadExt};
use common::{deserialize, protocol::{WireMessage, NodeToServer}};

pub async fn session_read_loop(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) {
    let mut buffer = String::new();

    loop {
        let n = reader.read_line(&mut buffer).await.unwrap_or(0);
        if n == 0 {
            break;
        }

        match deserialize::<WireMessage>(buffer.trim()) {
            WireMessage::NodeToServer(NodeToServer::Disconnect { .. }) => break,
            _ => {}
        }

        buffer.clear();
    }
}
