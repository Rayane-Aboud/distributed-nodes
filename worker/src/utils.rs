use std::os::unix::net::SocketAddr;

use tokio::net::TcpStream;

use crate::worker_node_info::PeerRegistry;


pub async fn handle_peer_session(
    socket: TcpStream,
    addr: SocketAddr,
    peers: PeerRegistry
){
    
}