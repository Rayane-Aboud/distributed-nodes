use std::{collections::HashMap, net::{SocketAddr}};

use common::{ serialize};
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

