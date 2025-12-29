use tokio::{net::TcpListener, sync::broadcast};

use crate::{utils::{cli_loop, handle_connection}, worker_node_info::WorkerRegistry};

pub async fn run_network(
    listener: TcpListener,
    tx: broadcast::Sender<String>,
    workers: WorkerRegistry,
) {
    loop {
        let (socket, addr) = listener.accept().await.unwrap();

        let rx = tx.subscribe();
        let tx = tx.clone();
        let workers = workers.clone();

        tokio::spawn(handle_connection(socket, addr, tx, rx, workers));
    }
}


pub async fn run_cli(workers: WorkerRegistry) {
    cli_loop(workers).await;
}
