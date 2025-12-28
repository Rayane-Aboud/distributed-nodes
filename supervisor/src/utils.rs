use std::net::SocketAddr;

use common::{Message, serialize};
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader}, net::TcpStream, sync::broadcast, time::Instant};

use crate::worker_node::{WorkerInfo, WorkerRegistry};

pub async fn handle_connection(
    socket: TcpStream,
    addr: SocketAddr,
    tx: broadcast::Sender<String>,
    mut rx: broadcast::Receiver<String>,
    workers: WorkerRegistry,
) {

    {
        // Register worker
        let mut map = workers.lock().await;
        map.insert(
            addr.to_string(),
            WorkerInfo {
                addr,
                connected_at: Instant::now(),
            },
        );
    }


    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();
    
    let _ = tx.send(serialize(Message::Connected(addr.to_string())));

    loop {
        tokio::select! {
            r = reader.read_line(&mut buffer) => {
                if r.unwrap_or(0) == 0 {
                    let _ = tx.send(
                        serialize(Message::Disconnected(addr.to_string()))
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

    {
        let mut map = workers.lock().await;
        map.remove(&addr.to_string());
    }
    let _ = tx.send(serialize(Message::Disconnected(addr.to_string())));
}



pub async fn cli_loop(workers: WorkerRegistry) {
    let stdin = io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();

        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
            break;
        }

        match line.trim(){
            "list" => {
                let map = workers.lock().await;
                println!("Connected workers:");
                for (id, info) in map.iter(){
                    println!(
                        "{} | connected {:?} ago",
                        id,
                        info.connected_at.elapsed()
                    );
                }
            }
            "count" => {
                let map = workers.lock().await;
                println!("Worker count: {}", map.len());
            }
            "help" => {
                println!("Commands:");
                println!("  list   - show connected workers");
                println!("  count  - number of workers");
                println!("  help   - show commands");
            }
            "" => {}

            _ => {
                println!("Unknown command. Type `help`.");
            }
        }
    }
}
