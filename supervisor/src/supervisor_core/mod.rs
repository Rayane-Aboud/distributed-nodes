mod admit_welcome;

use common::protocol::{SupervisorEvent};
use ed25519_dalek::{SigningKey};
use tokio::{sync::mpsc};
use crate::{supervisor_core::admit_welcome::admit_and_welcome, supervisor_node::SupervisorNode, worker_node_info::{WorkerRegistry}};


impl SupervisorNode {
    pub async fn run_supervisor_core(
        workers: WorkerRegistry,
        signing_key: SigningKey,
        supervisor_id: String,
        mut rx: mpsc::Receiver<SupervisorEvent>,
    ) {
        while let Some(event) = rx.recv().await {
            match event {
                SupervisorEvent::Admit { node_id, addr, tx, pub_key } => {
                    admit_and_welcome(
                        &workers,
                        signing_key.clone(),
                        &supervisor_id,
                        node_id,
                        addr,
                        pub_key,
                        tx,
                    ).await;
                }

                SupervisorEvent::Heartbeat { node_id, timestamp } => {
                    println!("received heartbeat message from node_id: {} at timestamp: {}",node_id, timestamp);
                }

                SupervisorEvent::Remove { node_id } => {
                    workers.lock().await.remove(&node_id);
                }

                SupervisorEvent::SendTo { node_id, msg } => {
                    if let Some(w) = workers.lock().await.get(&node_id) {
                        let _ = w.tx.send(msg).await;
                    }
                }

                SupervisorEvent::Broadcast { msg, except } => {
                    let map = workers.lock().await;
                    for (id, w) in map.iter() {
                        if except.as_deref() == Some(id) {
                            continue;
                        }
                        let _ = w.tx.send(msg.clone()).await;
                    }
                }
            }
        }
    }
}


