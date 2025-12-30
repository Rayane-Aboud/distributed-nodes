use std::{collections::HashMap, net::SocketAddr};

use common::{deserialize, protocol::{NodeToServer, ServerToNode, WireMessage}, serialize};
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{TcpStream}, sync::{Mutex, broadcast}, time::Instant};

use crate::worker_node_info::{WorkerInfo, WorkerRegistry};


async fn register_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    node_id: &str,
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



pub async fn handle_worker_session(
    socket: TcpStream,
    addr: SocketAddr,
    workers: WorkerRegistry,
) {

    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();

    // ---- expect HELLO ----
    if reader.read_line(&mut buffer).await.unwrap_or(0) == 0 {
        return;
    }

    let msg = deserialize::<WireMessage>(buffer.trim());
    let node_id = match msg {
        WireMessage::NodeToServer(NodeToServer::Hello { node_id }) => node_id,
        _ => return,
    };
    
    register_worker(&workers, &node_id, addr).await;
    println!("{:?}",workers);
    // ---- send WELCOME ----
    let welcome = WireMessage::ServerToNode(
        ServerToNode::Welcome { supervisor_id: "supervisor-1".to_string() }
    );

    write
        .write_all(format!("{}\n",serialize(welcome)).as_bytes())
        .await
        .ok();

    buffer.clear();

    loop {
        match reader.read_line(&mut buffer).await.unwrap_or(0) {
            0 => break,
            _ => {
                let msg = deserialize::<WireMessage>(buffer.trim());
                match msg {
                    WireMessage::NodeToServer(NodeToServer::Heartbeat { node_id, timestamp })=>{

                    }
                    WireMessage::NodeToServer(NodeToServer::Disconnect { reason }) => {
                        break;
                    }
                    _ => {
                        break; // protocol violation
                    }

                }
                buffer.clear();
            }
        }
    }

    remove_worker(&workers,addr).await;
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
