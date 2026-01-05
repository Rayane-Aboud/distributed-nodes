use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use common::protocol::WorkerEvent;
use ed25519_dalek::SigningKey;
use rand_core::OsRng;
use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, mpsc}};

use crate::{
    supervisor_session::SupervisorSession,
    worker_node_info::{PeerRegistry},
    worker_session::PeerSession,
};

pub struct WorkerNode {
    pub id: String,
    pub peer_listener: TcpListener,
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
        /* 
            create tx and rx for WorkerEvent (core worker logic)
            tx is passed to process...
        */
        let (tx_to_core_worker, rx_of_core_worker) = mpsc::channel(128);

        /* 
            distructuring Worker node to make it easy to manipulate the fields
        */
        let WorkerNode {
            id,
            supervisor_addr,
            peer_listener,
            peers,
            signing_key,
        } = self;

        /* 
            Connect to supervisor
            if failed stop the run function
        */
        let supervisor_stream = TcpStream::connect(supervisor_addr)
            .await
            .expect("failed to connect to supervisor");

        /*
            Start the supervisor session
            Role: receive and send messages between the worker node and the supervisor
            1. starts with handshake if it fails it panics
        */
        let tx_from_supervisor_to_core_worker = tx_to_core_worker.clone();
        tokio::spawn(async move {
            SupervisorSession::run(
                supervisor_stream,
                id,
                signing_key,
                tx_from_supervisor_to_core_worker,
            )
            .await;
        });

        /* 
            Start the worker session after receiving the welcome message from supervisor
            containing the data about other peers in the network
            1. worker receives and accepts connections 
            2. worker in parallel sends connection requests
            3. worker activates reader loop
            4. worker activates writer loop

        */
        let peers_run_inbound = peers.clone();
        let tx_from_peer_session_to_core_worker = tx_to_core_worker;
        tokio::spawn(async move {
            loop {
                let (socket, addr) = peer_listener.accept().await.unwrap();
                tokio::spawn(
                    PeerSession::run_inbound(
                        socket,
                        tx_from_peer_session_to_core_worker.clone(),
                        addr,
                        peers_run_inbound.clone(),
                    )
                );
            }
        });
        /* 
            Core logic:
            1. gives the order to send a message to a worker or a supervisor 
            2. give the order to remove or add a worker node
            3. receives the messages coming from Peers via tx peer session task and messages coming from Supervisor via supervisor_task 
        */
        Self::run_worker_core(rx_of_core_worker, peers).await;
    }

    async fn run_worker_core(
        mut rx: mpsc::Receiver<WorkerEvent>,
        peers: PeerRegistry,
    ) {
        while let Some(event) = rx.recv().await {
            match event {
                WorkerEvent::SupervisorWelcome { peers: initial_peers, .. } => {
                    println!("supervisor worker");
                    for peer in initial_peers {
                        let peer_id = peer.node_id.clone();

                        {
                            let mut map = peers.lock().await;
                            if map.contains_key(&peer_id) {
                                continue;
                            }
                            let info = peer.clone().into();
                            map.insert(peer_id.clone(), info);
                        }

                        // Connect to peer
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
                WorkerEvent::MessageFromPeer { message } => todo!(),
                
                }
        }
    }
}