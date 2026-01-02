use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::mpsc, task::JoinHandle};
use common::{protocol::WireMessage};

use crate::session::lifecycle::WorkerSession;

impl WorkerSession {
    pub fn spawn_writer(
            mut writer: OwnedWriteHalf,//this writer is from socket
    ) -> (mpsc::Sender<WireMessage>, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<WireMessage>(32);

        let handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let _ = writer
                    .write_all(format!("{}\n", common::serialize(msg)).as_bytes())
                    .await;
            }
        });

        (tx, handle)
    }

}