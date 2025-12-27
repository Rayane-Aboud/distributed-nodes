use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::broadcast;
use common::{Message, serialize};

pub struct SupervisorNode {
    listener: TcpListener,
    tx: broadcast::Sender<String>,
}

impl SupervisorNode {
    pub async fn new(addr: &str) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (tx, _) = broadcast::channel(128);

        Self { listener, tx }
    }

    pub async fn run(self) {
        loop {
            let (socket, addr) = self.listener.accept().await.unwrap();

            let tx = self.tx.clone();
            let rx = tx.subscribe();

            tokio::spawn(handle_connection(socket, addr.to_string(), tx, rx));
        }
    }
}


async fn handle_connection(
    socket: TcpStream,
    addr: String,
    tx: broadcast::Sender<String>,
    mut rx: broadcast::Receiver<String>,
) {
    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();

    let _ = tx.send(serialize(Message::Connected(addr.clone())));

    loop {
        tokio::select! {
            r = reader.read_line(&mut buffer) => {
                if r.unwrap_or(0) == 0 {
                    let _ = tx.send(
                        serialize(Message::Disconnected(addr.clone()))
                    );
                    break;
                }

                let _ = tx.send(
                    serialize(Message::Broadcast(buffer.trim().to_string()))
                );

                buffer.clear();
            }

            m = rx.recv() => {
                if let Ok(m) = m {
                    let _ = write
                        .write_all(format!("{}\n", m).as_bytes())
                        .await;
                }
            }
        }
    }
}


#[tokio::main]
async fn main() {
    let supervisor = SupervisorNode::new("127.0.0.1:9000").await;
    supervisor.run().await;
}
