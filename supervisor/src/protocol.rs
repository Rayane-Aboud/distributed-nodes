use tokio::io::{BufReader, AsyncBufReadExt};
use common::{deserialize, protocol::{WireMessage, NodeToServer}};

pub async fn recv_worker_hello(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Option<String> {
    let mut buf = String::new();
    let n = reader.read_line(&mut buf).await.ok()?;
    if n == 0 {
        return None;
    }

    match deserialize::<WireMessage>(buf.trim()) {
        WireMessage::NodeToServer(NodeToServer::Hello { node_id }) => Some(node_id),
        _ => None,
    }
}
