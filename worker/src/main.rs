use tokio::net::TcpStream;              // TCP client socket
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{sleep, Duration};     // Periodic heartbeat
use common::{Message, serialize};       // Shared protocol

#[tokio::main]
async fn main() {
    // Worker identifier passed via CLI
    let id = std::env::args().nth(1).unwrap_or("worker".into());

    // Connect to supervisor
    let socket = TcpStream::connect("127.0.0.1:9000").await.unwrap();

    // Split socket into read/write halves
    let (read, mut write) = socket.into_split();

    // Line-based reader
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();

    let read_id = id.clone();

    // Spawn task to receive supervisor messages
    tokio::spawn(async move {
        loop {
            if reader.read_line(&mut buffer).await.unwrap_or(0) == 0 {
                break;
            }
            println!("[{} RECEIVED] {}", read_id, buffer.trim());
            buffer.clear();
        }
    });

    loop {
        // Send heartbeat to supervisor
        let msg = serialize(Message::Ping(id.clone()));

        write.write_all(format!("{}\n", msg).as_bytes()).await.unwrap();

        // Fixed interval heartbeat
        sleep(Duration::from_secs(2)).await;
    }
}
