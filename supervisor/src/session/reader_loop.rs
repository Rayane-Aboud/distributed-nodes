
use tokio::{io::{AsyncBufReadExt, BufReader}, net::tcp::OwnedReadHalf, task::JoinHandle};
use common::{deserialize, protocol::{WireMessage, NodeToServer}};

use crate::session::lifecycle::WorkerSession;
impl WorkerSession {
    pub fn spawn_reader(
        mut reader: BufReader<OwnedReadHalf>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut buffer = String::new();
            loop {
                let n = reader.read_line(&mut buffer).await.unwrap_or(0);
                if n == 0 { break; }

                match deserialize::<WireMessage>(buffer.trim()) {
                    WireMessage::NodeToServer(NodeToServer::Disconnect { .. }) => break,
                    _ => {}
                }

                buffer.clear();
            }
        })

    }
    
}