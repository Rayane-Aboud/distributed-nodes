use tokio::net::TcpListener;           // TCP server socket
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::broadcast;            // Fan-out messaging
use common::{Message, serialize};      // Shared protocol

#[tokio::main]                         // Async runtime entry point
async fn main() {
    // Bind TCP listener to fixed address
    let listener = TcpListener::bind("127.0.0.1:9000").await.unwrap();

    // Broadcast channel allows one sender to reach many receivers
    let (tx, _) = broadcast::channel::<String>(128);

    loop {
        // Accept incoming TCP connection
        let (socket, addr) = listener.accept().await.unwrap();

        // Clone sender so each connection can publish
        let tx = tx.clone();

        // Each connection gets its own receiver
        let mut rx = tx.subscribe();

        // Spawn independent task per worker
        tokio::spawn(async move {
            // Split socket into read/write halves
            let (read, mut write) = socket.into_split();

            // Wrap reader for line-based protocol
            let mut reader = BufReader::new(read);
            let mut buffer = String::new();

            // Notify all nodes of new connection
            let _ = tx.send(serialize(Message::Connected(addr.to_string())));

            loop {
                tokio::select! {
                    // Handle inbound worker message
                    read = reader.read_line(&mut buffer) => {
                        if read.unwrap_or(0) == 0 {
                            // Worker disconnected
                            let _ = tx.send(
                                serialize(Message::Disconnected(addr.to_string()))
                            );
                            break;
                        }

                        // Relay worker message to all peers
                        let _ = tx.send(
                            serialize(Message::Broadcast(buffer.trim().to_string()))
                        );

                        buffer.clear();
                    }

                    // Receive broadcast from supervisor channel
                    msg = rx.recv() => {
                        if let Ok(msg) = msg {
                            // Send broadcast to this worker
                            let _ = write.write_all(format!("{}\n", msg).as_bytes()).await;
                        }
                    }
                }
            }
        });
    }
}
