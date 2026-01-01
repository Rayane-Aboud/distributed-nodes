use tokio::io::BufReader;

use crate::utils::recv_worker_hello;

pub async fn handshake(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Option<String> {
    recv_worker_hello(reader).await
}
