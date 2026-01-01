use tokio::{sync::mpsc, io::AsyncWriteExt};
use common::{serialize, protocol::WireMessage};

pub fn spawn_writer(
    write: tokio::net::tcp::OwnedWriteHalf,
) -> mpsc::Sender<WireMessage> {
    let (tx, mut rx) = mpsc::channel::<WireMessage>(32);

    tokio::spawn(async move {
        let mut writer = write;
        while let Some(msg) = rx.recv().await {
            let _ = writer
                .write_all(format!("{}\n", serialize(msg)).as_bytes())
                .await;
        }
    });

    tx
}
