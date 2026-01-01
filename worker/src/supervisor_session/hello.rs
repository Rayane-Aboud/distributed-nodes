use common::protocol::{NodeToServer, WireMessage};
use common::serialize;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::io::AsyncWriteExt;


pub async fn send_hello(id: &str, write: &mut OwnedWriteHalf) {
    let hello = WireMessage::NodeToServer(NodeToServer::Hello { node_id: id.to_string() });

    write
        .write_all(format!("{}\n", serialize(hello)).as_bytes())
        .await
        .unwrap();
}
