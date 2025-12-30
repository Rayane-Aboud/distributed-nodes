pub mod protocol;


use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn serialize<T: Serialize>(msg: T) -> String {
    serde_json::to_string(&msg).unwrap()
}

pub fn deserialize<T: DeserializeOwned>(s: &str) -> T {
    serde_json::from_str(s).unwrap()
}