use tokio::{net::{TcpStream}, time::Instant};

use crate::tasks::{worker_read_loop, worker_write_loop};                    // Async TCP client



pub struct WorkerNode {
    id: String,
    supervisor: TcpStream,
    start: Instant
}


impl WorkerNode {
    pub async fn new(id: String, supervisor_addr: &str) -> Self {
        let supervisor = TcpStream::connect(supervisor_addr).await.unwrap();
        let start = Instant::now();
        Self {id, supervisor,start}
    }


    pub async fn run(self) {
        let (read, write) = self.supervisor.into_split();

        let reader = tokio::spawn(worker_read_loop(read));
        let start = self.start;
        let writer = tokio::spawn(worker_write_loop(self.id, write, start));

        let _ = tokio::join!(reader, writer);
    }
}
