
use common::protocol::NodeToServer;
use tokio::{io::BufReader, net::tcp::OwnedReadHalf};

use crate::{session::lifecycle::WorkerSession, utils::recv_worker_hello};

impl WorkerSession {
    pub async fn handshake(
        reader:&mut BufReader<OwnedReadHalf>,
    ) ->  Option<NodeToServer> {
        let node_to_server_hello = recv_worker_hello( reader).await?;
        Some(node_to_server_hello)
    }
}