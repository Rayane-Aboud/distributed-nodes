use std::net::SocketAddr;

use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeToServer {
    Hello {
        node_id: String,
    },

    Heartbeat {
        node_id: String,
        timestamp:u64
    },

    Disconnect {
        reason: String,
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerToNode {
    Welcome {
        supervisor_id: String,
    },

    NewPeer {
        node: PeerInfoMessage,
    },

    Command {
        payload: String,
    },

    Shutdown {
        reason:String,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfoMessage {
    pub node_id: String,
    pub addr:SocketAddr
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeToNode {
    PeerHello {
        node_id: String,
    },
    PeerMessage {
        from: String,
        payload: String,
    },
    JoinMessage {
        id: String,
        addr: SocketAddr
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerToServer {
    Sync {
        supervisor_id: String,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    NodeToServer(NodeToServer),
    ServerToNode(ServerToNode),
    NodeToNode(NodeToNode),
    ServerToServer(ServerToServer),
}
