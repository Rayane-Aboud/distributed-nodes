use std::{collections::HashMap, net::{SocketAddr}};

use common::{Message, serialize};
use tokio::{io::BufReader, net::TcpStream, sync::{Mutex, broadcast}, time::Instant};

use crate::worker_node_info::{WorkerInfo, WorkerRegistry};

async fn register_neighbour(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    addr: SocketAddr
){
    let mut map = workers.lock().await;
    map.insert(
        addr.to_string(),
        WorkerInfo { 
            addr,
            connected_at: Instant::now() 
         },
    );
}


async fn remove_worker(
    workers: &Mutex<HashMap<String, WorkerInfo>>,
    addr: SocketAddr
){
    let mut map = workers.lock().await;
    map.remove(&addr.to_string());
}

async fn handle_read_from_neighbour()->bool{
    true
}

async fn handle_write_from_neighbour(){

}

async fn handle_read_from_supervisor()->bool{
    true
}

async fn handle_write_from_supervisor(){

}

pub async fn handle_connection(
    socket: TcpStream,
    addr: SocketAddr,
    tx: broadcast::Sender<String>,
    mut rx: broadcast::Receiver<String>,
    workers: WorkerRegistry
){
    register_neighbour(&workers, addr).await;
    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut buffer = String::new();
    let addr_str = &addr.to_string();
    let _ = tx.send(serialize(Message::Connected(addr.to_string())));

    loop {
        tokio::select! {
            ok_read_supervisor = handle_read_from_supervisor() =>{
                if !ok_read_supervisor {break;}
            }

            ok_read_neighbour = handle_read_from_neighbour() => {
                if !ok_read_neighbour {break;}
            }

            _  = handle_write_from_supervisor() => {}

            __ = handle_write_from_neighbour() => {}
        }
    }

    remove_worker(&workers, addr).await;

}