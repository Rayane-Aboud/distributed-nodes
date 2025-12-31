use std::{collections::HashMap, net::SocketAddr};

use common::{deserialize, protocol::{NodeToServer, ServerToNode, WireMessage}, serialize};
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader}, net::TcpStream, sync::{Mutex, mpsc}, time::Instant};

use crate::worker_node_info::{WorkerInfo, WorkerRegistry};


async fn register_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    node_id: &str,
    addr: SocketAddr,
    tx: mpsc::Sender<WireMessage>,
) {
    let mut map = workers.lock().await;
    map.insert(
        addr.to_string(),
        WorkerInfo {
            addr,
            connected_at: Instant::now(),
            tx,
        },
    );
}

pub async fn remove_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    node_id: &str,
) {
    let mut map = workers.lock().await;
    map.remove(node_id);
}

async fn setup_worker(
    workers: &WorkerRegistry,
    node_id: String,
    addr: SocketAddr,
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

    register_worker(workers, &node_id, addr, tx.clone()).await;

    tx
}


pub async fn handle_worker_session(
    socket: TcpStream,
    addr: SocketAddr,
    workers: WorkerRegistry,
) {
    let (read, write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();


    // --- expect HELLO ---
    if reader.read_line(&mut buffer).await.unwrap_or(0) == 0 {
        return;
    }

    let msg = deserialize::<WireMessage>(buffer.trim());
    let node_id = match msg {
        WireMessage::NodeToServer(NodeToServer::Hello { node_id }) => node_id,
        _ => return,
    };
    buffer.clear();


    // --- setup worker: channel, writer task, registry ---
    let tx = setup_worker(&workers, node_id.clone(), addr, write).await;



    // --- send WELCOME ---
    let welcome = WireMessage::ServerToNode(ServerToNode::Welcome {
        supervisor_id: "supervisor-1".to_string(),
    });
    let _ = tx.send(welcome).await;


    // --- main read loop ---
    loop {
        let n = reader.read_line(&mut buffer).await.unwrap_or(0);
        if n == 0 {
            break;
        }

        let msg = deserialize::<WireMessage>(buffer.trim());
        match msg {
            WireMessage::NodeToServer(NodeToServer::Heartbeat { .. }) => {}
            WireMessage::NodeToServer(NodeToServer::Disconnect { .. }) => break,
            _ => break, // protocol violation
        }

        buffer.clear();
    }

    // --- cleanup ---
    remove_worker(&workers, &node_id).await;
    
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
