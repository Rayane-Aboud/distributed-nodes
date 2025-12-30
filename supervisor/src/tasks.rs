use tokio::{net::TcpListener};

use crate::{utils::{cli_loop, handle_worker_session}, worker_node_info::WorkerRegistry};

pub async fn accept_node_connections(
    listener: TcpListener,
    workers: WorkerRegistry,
) {
    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        let workers = workers.clone();

        tokio::spawn(handle_worker_session(socket, addr, workers));
    }
}


pub async fn run_cli(workers: WorkerRegistry) {
    cli_loop(workers).await;
}
