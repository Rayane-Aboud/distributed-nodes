use std::net::SocketAddr;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;

/// Node-to-supervisor messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeToServer {
    Hello {
        node_id: String,
        pub_key: Vec<u8>,           
        signature: Option<Vec<u8>>, 
    },
    Heartbeat {
        node_id: String,
        timestamp: u64,
        signature: Option<Vec<u8>>, 
    },
    Disconnect {
        reason: String,
        signature: Option<Vec<u8>>,
    },
}

/// Supervisor-to-node messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerToNode {
    Welcome {
        supervisor_id: String,
        supervisor_pub_key: Vec<u8>,     
        peers: Vec<PeerInfoMessage>,       
        signature: Vec<u8>,         
    },
    NewPeer {
        node: PeerInfoMessage,
        signature: Vec<u8>,
    },
    Command {
        payload: String,
        signature: Vec<u8>,
    },
    Shutdown {
        reason: String,
        signature: Vec<u8>,
    },
}

/// Metadata about a peer node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfoMessage {
    pub node_id: String,
    pub addr: SocketAddr,
    pub pub_key: Vec<u8>,       
    pub signature: Vec<u8>,
}

/// Messages between worker nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeToNode {
    PeerHello {
        node_id: String,
        signature: Option<Vec<u8>>,
    },
    PeerMessage {
        from: String,
        payload: String,
        signature: Vec<u8>,
    },
    JoinMessage {
        id: String,
        addr: SocketAddr,
        pub_key: Vec<u8>,
        signature: Vec<u8>,
    },
}

/// Supervisor-to-supervisor messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerToServer {
    Sync {
        supervisor_id: String,
        signature: Vec<u8>, // Signed by sending supervisor
    },
}

/// Unified wire message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    NodeToServer(NodeToServer),
    ServerToNode(ServerToNode),
    NodeToNode(NodeToNode),
    ServerToServer(ServerToServer),
}

/// Supervisor internal events
pub enum SupervisorEvent {
    Admit {
        node_id: String,
        addr: SocketAddr,
        tx: mpsc::Sender<WireMessage>,
        pub_key: Vec<u8>,     
    },
    Remove {
        node_id: String,
    },
    SendTo {
        node_id: String,
        msg: WireMessage,
    },
    Heartbeat {
        node_id: String,
        timestamp: u64,
    },

    Broadcast {
        msg: WireMessage,
        except: Option<String>,
    },
}

pub enum WorkerEvent {
    SupervisorWelcome {
        supervisor_id: String,
        peers: Vec<PeerInfoMessage>,
    },
    NewPeer {
        peer: PeerInfoMessage,
    },
    SupervisorShutdown {
        reason: String,
    },
}

