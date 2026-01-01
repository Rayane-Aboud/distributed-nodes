use common::protocol::WireMessage;
use tokio::{sync::mpsc, time::Instant};

use crate::worker_node_info::{WorkerInfo, WorkerRegistry};



pub enum SupervisorEvent {
    Admit {
        node_id: String,
        addr: std::net::SocketAddr,
        tx: mpsc::Sender<WireMessage>
    },
    Remove {
        node_id: String,
    },
    SendTo {
        node_id: String,
        msg: WireMessage
    },
    Broadcast {
        msg: WireMessage,
        except: Option<String>
    }
}


pub async fn run_supervisor_core(
    workers: WorkerRegistry,
    mut rx: mpsc::Receiver<SupervisorEvent>
){
    //receives event
    while let Some(event) = rx.recv().await {
        match event {
            SupervisorEvent::Admit { node_id, addr, tx } => {
                //if new: registration of the node
                workers.lock().await.insert(
                    node_id,
                    WorkerInfo { 
                        addr, 
                        connected_at: Instant::now(), 
                        tx //creation of a writer channel to the worker 
                    }
                );
            }
            SupervisorEvent::Remove { node_id } => {
                //if remove: removal from registry
                workers.lock().await.remove(&node_id);
            }
            SupervisorEvent::SendTo { node_id, msg } =>{
                //if sending message
                if let Some(w) = workers.lock().await.get(&node_id) {
                    //use the tx of that worker to send
                    let _ = w.tx.send(msg).await;
                }
            }
            SupervisorEvent::Broadcast { msg, except } => {
                let map = workers.lock().await;
                for (id, w) in map.iter() {
                    if except.as_deref() == Some(id) {
                        continue;
                    }
                    //send to all workers except for the one responsible fr broadcast generation
                    let _ = w.tx.send(msg.clone()).await;
                }
            }
        }
    }
}