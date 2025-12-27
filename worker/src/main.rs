use tokio::net::TcpStream;                    // Async TCP client
use tokio::io::{                             // Async I/O traits
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};
use tokio::time::{sleep, Duration};           // Timers
use common::{Message, serialize};             // Shared protocol

/// Represents one worker process.
/// Owns identity and socket halves.
struct WorkerNode {
    id: String,                               // Logical node identity
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl WorkerNode {
    /// Constructor.
    /// Establishes TCP connection and initializes state.
    async fn new(id: String, addr: &str) -> Self {
        // Connect to supervisor
        let socket = TcpStream::connect(addr).await.unwrap();

        // Split socket into owned halves
        let (read, write) = socket.into_split();

        Self {
            id,
            reader: BufReader::new(read),
            writer: write,
        }
    }

    /// Main execution loop.
    /// Spawns read task and runs heartbeat loop.
    async fn run(mut self) {
        // Local buffers must live inside the async context
        let mut buffer = String::new();

        // Clone immutable identity for the read task
        let read_id = self.id.clone();

        // Take reader ownership into the spawned task
        let mut reader = self.reader;

        tokio::spawn(async move {
            loop {
                // Read one line from supervisor
                if reader.read_line(&mut buffer).await.unwrap_or(0) == 0 {
                    break;
                }

                // Handle inbound message
                println!("[{} RECEIVED] {}", read_id, buffer.trim());

                buffer.clear();
            }
        });

        // Heartbeat loop stays in main task
        loop {
            let msg = serialize(Message::Ping(self.id.clone()));

            self.writer
                .write_all(format!("{}\n", msg).as_bytes())
                .await
                .unwrap();

            sleep(Duration::from_secs(2)).await;
        }
    }
}

#[tokio::main]
async fn main() {
    // Resolve worker identity from CLI
    let id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "worker".to_string());

    // Construct node
    let worker = WorkerNode::new(id, "127.0.0.1:9000").await;

    // Run node lifecycle
    worker.run().await;
}
