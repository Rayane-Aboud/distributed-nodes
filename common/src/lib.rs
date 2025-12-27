pub enum Message {
    Ping(String),          // Worker heartbeat
    Connected(String),     // Worker joined
    Disconnected(String),  // Worker left
    Broadcast(String),     // Supervisor broadcast
}



pub fn serialize(msg: Message) -> String {
    match msg {
        Message::Ping(id) => format!("PING {}", id),
        Message::Connected(id) => format!("CONNECTED {}", id),
        Message::Disconnected(id) => format!("DISCONNECTED {}", id),
        Message::Broadcast(body) => format!("BROADCAST {}", body),
    }
}