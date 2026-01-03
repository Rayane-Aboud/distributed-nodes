pub mod protocol;


use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;

pub fn serialize<T: Serialize>(msg: T) -> String {
    serde_json::to_string(&msg).unwrap()
}


pub fn deserialize<T: DeserializeOwned>(s: &str) -> Option<T> {
    match serde_json::from_str(s) {
        Ok(val) => Some(val),
        Err(err) => {
            eprintln!("[deserialize] failed: {}", err);
            None
        }
    }
}
