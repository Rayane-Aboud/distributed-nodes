# Distributed Nodes

A supervisor and worker cluster written in Rust over plain TCP. I wrote my own handshake so that new
workers can register with the supervisor, find out about the other workers, and then talk to them
directly.

## How it's organised

- The **supervisor** keeps track of the cluster: which nodes are connected, and who needs to be told when
  a new one joins.
- A **worker** connects to the supervisor, then opens direct connections to the other workers.
- **common** holds the shared types and the messages of the protocol.

### What happens when a worker joins

1. **Handshake.** The worker sends `NodeToServer::Hello` and the supervisor answers with
   `ServerToNode::Welcome`.
2. **Discovery.** The supervisor sends the new worker the list of current peers (`ServerToNode::PeerList`),
   and tells the existing workers that someone joined (`ServerToNode::PeerJoin`).
3. **Connection.** The new worker updates its list of peers and connects to them, while the existing
   workers note the newcomer in a temporary registry and wait for its connection.

## Project structure

```
distributed-nodes/
├── common/         # shared types and protocol messages
├── supervisor/     # the supervisor
├── worker/         # the worker
├── Cargo.toml      # workspace configuration
├── start.sh        # launch script
└── readme.md
```

## Running it

You need a stable Rust toolchain.

```bash
git clone https://github.com/Rayane-Aboud/distributed-nodes.git
cd distributed-nodes
cargo build --release
```

Start the supervisor, then as many workers as you like, each one in its own terminal:

```bash
cargo run --release --bin supervisor
cargo run --release --bin worker      # run this in more terminals for more workers
```

Each new worker says `Hello`, gets the list of peers and connects to the workers that are already there.

## Where it's going

Done so far:

- the supervisor and worker roles
- registration and the handshake
- peer discovery

In progress: managing connections over time, and logging with condition variables.

Next: a complete logging system, checking that peer connections are really up, fault tolerance, and
sharing tasks between workers.

## Author

Rayane Aboud, Backend & AI Engineer ([rayaneaboud.dev](https://rayaneaboud.dev)).
