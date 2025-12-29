use std::{collections::HashMap, net::SocketAddr};

use common::{Message, serialize};
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{Mutex, broadcast}, time::Instant};

use crate::worker_node_info::{WorkerInfo, WorkerRegistry};


async fn register_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    addr: SocketAddr,
) {
    let mut map = workers.lock().await;
    map.insert(
        addr.to_string(),
        WorkerInfo {
            addr,
            connected_at: Instant::now(),
        },
    );
}

async fn remove_worker (
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    addr: SocketAddr,
){
    let mut map = workers.lock().await;
    map.remove(&addr.to_string());
}

async fn handle_read(
    reader: &mut BufReader<OwnedReadHalf>,
    buffer: &mut String,
    tx: &broadcast::Sender<String>,
    addr: &str,
) -> bool {
    match reader.read_line(buffer).await.unwrap_or(0) {
        0 => {
            let _ = tx.send(serialize(Message::Disconnected(addr.to_string())));
            false
        }
        _ => {
            let _ = tx.send(serialize(Message::Broadcast(
                buffer.trim().to_string(),
            )));
            buffer.clear();
            true
        }
    }
}


async fn handle_write(
    writer: &mut OwnedWriteHalf,
    rx: &mut broadcast::Receiver<String>,
) {
    if let Ok(msg) = rx.recv().await {
        let _ = writer
            .write_all(format!("{}\n", msg).as_bytes())
            .await;
    }
}




pub async fn handle_connection(
    socket: TcpStream,
    addr: SocketAddr,
    tx: broadcast::Sender<String>,
    mut rx: broadcast::Receiver<String>,
    workers: WorkerRegistry,
) {

    register_worker(&workers, addr).await;

    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();
    let addr_str = &addr.to_string();
    let _ = tx.send(serialize(Message::Connected(addr.to_string())));

    loop {
        tokio::select! {
            ok = handle_read(&mut reader, &mut buffer, &tx, addr_str) => {
                if !ok { break; }
            }

            _ = handle_write(&mut write, &mut rx) => {}
        }
    }

    remove_worker(&workers,addr).await;
    
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
