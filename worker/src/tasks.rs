use tokio::net::TcpListener;

use tokio::sync::broadcast;

use crate::worker_node_info::WorkerRegistry;

pub async fn run_network(
    listener: TcpListener,
    tx: broadcast::Sender<String>,
    workers: WorkerRegistry
) {
    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        let rx = tx.subscribe();
        let tx = tx.clone();
        let workers = workers.clone();

        tokio::spawn(handle_connection(socket,addr,tx,rx,workers));
    }
}