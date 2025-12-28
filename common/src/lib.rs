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


pub fn parse_message(input: &str) -> Option<Message> {
    let mut parts = input.splitn(2, ' ');
    let kind = parts.next()?;
    let rest = parts.next().unwrap_or("");

    match kind {
        "PING" => Some(Message::Ping(rest.to_string())),
        "CONNECTED" => Some(Message::Connected(rest.to_string())),
        "DISCONNECTED" => Some(Message::Disconnected(rest.to_string())),
        "BROADCAST" => Some(Message::Broadcast(rest.to_string())),
        _ => None,
    }
    
}