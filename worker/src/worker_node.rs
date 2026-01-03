use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use common::protocol::WorkerEvent;
use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, mpsc}, time::Instant};

use crate::{supervisor_session::SupervisorSession, worker_node_info::{PeerInfo, PeerRegistry}, worker_session::PeerSession};
use ed25519_dalek::SigningKey;
use rand_core::OsRng;


pub struct WorkerNode {
    pub id: String,

    // inbound peer connections only
    pub peer_listener: TcpListener,

    // outbound supervisor session
    pub supervisor_addr: SocketAddr,

    pub peers: PeerRegistry,
    pub signing_key: SigningKey,
}


impl WorkerNode {
    pub async fn new(
        id: String,
        supervisor_addr: &str,
        listen_addr: SocketAddr,
    ) -> Self {
        let supervisor_addr = supervisor_addr
            .parse()
            .expect("invalid supervisor address");

        let peer_listener = TcpListener::bind(listen_addr)
            .await
            .expect("failed to bind peer listener");

        let signing_key = {
            let mut csprng = OsRng;
            SigningKey::generate(&mut csprng)
        };

        let peers = Arc::new(Mutex::new(HashMap::new()));

        Self {
            id,
            supervisor_addr,
            peer_listener,
            peers,
            signing_key,
        }
    }



    pub async fn run(self) {
        // internal worker event channel
        let (tx, rx) = mpsc::channel(128);

        // destructure self ONCE
        let WorkerNode {
            id,
            supervisor_addr,
            peer_listener,
            peers,
            signing_key,
        } = self;

        // connect to supervisor (outbound)
        let supervisor_stream = TcpStream::connect(supervisor_addr)
            .await
            .expect("failed to connect to supervisor");

        // spawn supervisor session
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            SupervisorSession::run(
                supervisor_stream,
                id,
                signing_key,
                tx_clone,
            )
            .await;
        });

        // spawn peer listener (inbound)
        let peers_clone = peers.clone();
        tokio::spawn(async move {
            loop {
                let (socket, addr) = peer_listener.accept().await.unwrap();
                tokio::spawn(
                    PeerSession::run_inbound(
                        socket,
                        addr,
                        peers_clone.clone(),
                    )
                );
            }
        });

        // worker core loop (handles WorkerEvent)
        Self::run_worker_core(rx, peers).await;
    }


}



impl WorkerNode {
    pub async fn run_worker_core(
        mut rx: mpsc::Receiver<WorkerEvent>,
        peers: PeerRegistry,
    ) {
        while let Some(event) = rx.recv().await {
            match event {
                WorkerEvent::SupervisorWelcome { peers: initial_peers, .. } => {
                    // Insert all known peers
                    println!("supervisor worker");
                    for peer in initial_peers {
                        let peer_id = peer.node_id.clone();

                        {
                            let mut map: tokio::sync::MutexGuard<'_, HashMap<String, PeerInfo>> = peers.lock().await;
                            if map.contains_key(&peer_id) {
                                continue;
                            }
                            let info = peer.clone().into();
                            map.insert(peer_id.clone(), info);
                        }

                        // Spawn outbound connection
                        tokio::spawn(async move {
                            PeerSession::run_outbound(peer).await;
                        });
                    }
                }

                WorkerEvent::NewPeer { peer } => {
                    let peer_id = peer.node_id.clone();

                    {
                        let mut map = peers.lock().await;
                        if map.contains_key(&peer_id) {
                            continue;
                        }
                        let info = peer.clone().into();
                        map.insert(peer_id.clone(), info);
                    }
                    println!("new peer added:{:?}", peer);
                    tokio::spawn(async move {
                        PeerSession::run_outbound(peer).await;
                    });
                }

                WorkerEvent::SupervisorShutdown { reason } => {
                    // Hard stop for now
                    eprintln!("Supervisor shutdown: {}", reason);
                    break;
                }
            }
        }
    }
}
