use std::net::SocketAddr;
use tokio::{net::{TcpListener, TcpStream}, sync::broadcast};                    // Async TCP client



struct WorkerNode {
    id: String,
    listener: TcpListener,
    server_socket: TcpStream,
    tx: broadcast::Sender<String>,
}

impl WorkerNode {
    async fn new(id: String, addr: &str) -> Self {
        
        let server_socket = TcpStream::connect(addr).await.unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        //channel only used to broadcast heartbeat
        let (tx, _) = broadcast::channel(128);

        Self {
            id,
            listener,
            server_socket,
            tx
        }
    }

    async fn run(self) {
        let net = tokio::spawn(run_network(
            
        ));
        
    }
}
