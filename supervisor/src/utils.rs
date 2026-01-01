use std::{collections::HashMap, net::SocketAddr};

use common::{deserialize, protocol::{NodeToServer, PeerInfoMessage, ServerToNode, WireMessage}, serialize};
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader}, net::TcpStream, sync::{Mutex, mpsc}, time::Instant};

use crate::worker_node_info::{WorkerInfo, WorkerRegistry};


async fn insert_worker(
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

    insert_worker(workers, &node_id, addr, tx.clone()).await;

    tx
}


pub async fn recv_worker_hello(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Option<String> {
    let mut buffer = String::new();

    let n = reader.read_line(&mut buffer).await.ok()?; // return None if read fails
    if n == 0 {
        return None; // connection closed
    }

    match deserialize::<WireMessage>(buffer.trim()) {
        WireMessage::NodeToServer(NodeToServer::Hello { node_id }) => Some(node_id),
        _ => None, // unexpected message
    }
}


pub async fn register_and_emit_welcome(
    workers: &WorkerRegistry,
    node_id: String,
    addr: SocketAddr,
    write: tokio::net::tcp::OwnedWriteHalf,
    supervisor_id: &str,
) -> mpsc::Sender<WireMessage> {
    // 1. create channel
    let (tx, mut rx) = tokio::sync::mpsc::channel::<WireMessage>(32);

    // 2. spawn writer task
    tokio::spawn(async move {
        let mut writer = write;
        while let Some(msg) = rx.recv().await {
            let _ = writer
                .write_all(format!("{}\n", serialize(msg)).as_bytes())
                .await;
        }
    });

    // 3. register worker in registry
    insert_worker(workers, &node_id, addr, tx.clone()).await;

    // 4. send Welcome immediately
    let welcome = WireMessage::ServerToNode(ServerToNode::Welcome {
        supervisor_id: supervisor_id.to_string(),
    });
    let _ = tx.send(welcome).await;

    tx
}

pub async fn send_peer_list(
    workers: &WorkerRegistry,
    node_id: &str,
    tx: &mpsc::Sender<WireMessage>,
) {
    let workers_lock = workers.lock().await;
    for (existing_id, worker) in workers_lock.iter() {
        if existing_id == node_id {
            continue;
        }
        let peer_info = PeerInfoMessage {
            node_id: existing_id.clone(),
            addr: worker.addr,
        };
        let _ = tx.send(WireMessage::ServerToNode(ServerToNode::NewPeer {
            node: peer_info,
        })).await;
    }
}


pub async fn broadcast_new_peer(
    workers: &WorkerRegistry,
    node_id: &str,
    addr: SocketAddr,
) {
    let new_peer_info = PeerInfoMessage {
        node_id: node_id.to_string(),
        addr,
    };
    let workers_lock = workers.lock().await;
    for (existing_id, worker) in workers_lock.iter() {
        if existing_id == node_id {
            continue;
        }
        let _ = worker.tx.send(WireMessage::ServerToNode(ServerToNode::NewPeer {
            node: new_peer_info.clone(),
        })).await;
    }
}


pub async fn run_worker_lifecycle(
    socket: TcpStream,
    addr: SocketAddr,
    workers: WorkerRegistry,
) {
    let (read, write) = socket.into_split();
    let mut reader = BufReader::new(read);

    // --- read Hello ---
    let node_id = match recv_worker_hello(&mut reader).await {
        Some(id) => id,
        None => return,
    };

    // --- register worker + spawn writer + send Welcome ---
    let tx = register_and_emit_welcome(&workers, node_id.clone(), addr, write, "supervisor-1").await;

    // --- send PeerList to new node ---
    send_peer_list(&workers, &node_id, &tx).await;

    // --- broadcast NewPeer to existing nodes ---
    broadcast_new_peer(&workers, &node_id, addr).await;

    // --- main read loop ---
    let mut buffer: String = String::new();
    loop {
        let n = reader.read_line(&mut buffer).await.unwrap_or(0);
        if n == 0 { break; }

        match deserialize::<WireMessage>(buffer.trim()) {
            WireMessage::NodeToServer(NodeToServer::Heartbeat { .. }) => {}
            WireMessage::NodeToServer(NodeToServer::Disconnect { .. }) => break,
            _ => break,
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
