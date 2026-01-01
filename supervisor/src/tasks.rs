use tokio::{net::TcpListener};

use crate::{utils::{cli_loop, run_worker_lifecycle}, worker_node_info::WorkerRegistry};

pub async fn handle_worker_nodes(
    listener: TcpListener,
    workers: WorkerRegistry,
) {
    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        let workers = workers.clone();

        tokio::spawn(run_worker_lifecycle(socket, addr, workers));
    }
}


pub async fn run_cli(workers: WorkerRegistry) {
    cli_loop(workers).await;
}
