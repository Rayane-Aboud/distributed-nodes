use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};
use ed25519_dalek::SigningKey;
use rand_core::OsRng;

use crate::{
    supervisor_session::SupervisorSession,
    worker_node_info::{PeerInfo, PeerRegistry},
    worker_session::PeerSession,
};
use common::protocol::{WorkerEvent};

pub struct WorkerNode {
    pub id: String,
    pub peer_listener: TcpListener,
    pub supervisor_addr: SocketAddr,
    pub peers: PeerRegistry,
    pub signing_key: SigningKey,
}

impl WorkerNode {
    pub async fn new(id: String, supervisor_addr: &str, listen_addr: SocketAddr) -> Self {
        let supervisor_addr = supervisor_addr.parse().expect("invalid supervisor address");
        let peer_listener = TcpListener::bind(listen_addr)
            .await
            .expect("failed to bind peer listener");

        let signing_key = SigningKey::generate(&mut OsRng);
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
        // Create channel for core worker
        let (tx_core, rx_core) = mpsc::channel(128);

        // Split WorkerNode fields for convenience
        let WorkerNode {
            id,
            supervisor_addr,
            peer_listener,
            peers,
            signing_key,
        } = self;

        // -------------------------------
        // 1. Supervisor session task
        // -------------------------------
        let tx_core_for_supervisor = tx_core.clone();
        tokio::spawn(async move {
            let supervisor_stream = TcpStream::connect(supervisor_addr)
                .await
                .expect("failed to connect to supervisor");

            SupervisorSession::run(
                supervisor_stream,
                id,
                signing_key,
                tx_core_for_supervisor,
            )
            .await;
        });

        // -------------------------------
        // 2. Peer listener task
        // -------------------------------
        let tx_core_for_peers = tx_core.clone();
        tokio::spawn(async move {
            loop {
                let (socket, addr) = peer_listener.accept().await.unwrap();
                tokio::spawn(PeerSession::run_acceptor(
                    socket,
                    tx_core_for_peers.clone(),
                    addr,
                ));
            }
        });

        // -------------------------------
        // 3. Core worker loop
        // -------------------------------
        Self::run_core(rx_core, peers).await;
    }

    async fn run_core(mut rx: mpsc::Receiver<WorkerEvent>, peers: PeerRegistry) {
        while let Some(event) = rx.recv().await {
            match event {
                WorkerEvent::SupervisorWelcome { peers: initial_peers, .. } => {
                    let mut map = peers.lock().await;

                    for peer in initial_peers {
                        if map.contains_key(&peer.node_id) {
                            continue;
                        }

                        map.insert(
                            peer.node_id.clone(),
                            PeerInfo {
                                node_id: peer.node_id.clone(),
                                addr: peer.addr,
                                pub_key: peer.pub_key,
                                signature: Some(peer.signature),
                                tx: None, 
                            },
                        );
                    }
                }


                WorkerEvent::PeerHelloReceived {
                    node_id,
                    addr,
                    pub_key,
                    signature,
                    tx,
                    admit_tx,
                } => {
                    let mut map = peers.lock().await;

                    match map.get_mut(&node_id) {
                        Some(peer) => {
                            peer.tx = Some(tx.clone());
                        }
                        None => {
                            
                            map.insert(
                                node_id.clone(),
                                PeerInfo {
                                    node_id,
                                    addr,
                                    pub_key,
                                    signature,
                                    tx: Some(tx.clone()),
                                },
                            );
                        }
                    }

                    let _ = admit_tx.send(());
                }


                WorkerEvent::NewPeer { peer } => {
                    let mut map = peers.lock().await;
                    if map.contains_key(&peer.node_id) {
                        continue;
                    }
                    
                    map.insert(peer.node_id.clone(), PeerInfo::new(peer.clone()));
                    println!("New peer added by supervisor: {}", peer.node_id);
                }

                WorkerEvent::SupervisorShutdown { reason } => {
                    eprintln!("Supervisor shutdown: {}", reason);
                    break;
                }

                WorkerEvent::MessageFromPeer { message } => {
                    println!("Message from peer: {}", message);
                    // Handle or route message as needed
                }
            }
        }
    }
}
