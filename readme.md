# Distributed Nodes

A Rust-based framework implementing a supervisor-worker architecture for building scalable and fault-tolerant node clusters.

## Overview

Distributed Nodes allows creating distributed systems in Rust. It provides a supervisor node that coordinates workers, handles peer discovery, and manages communication across a cluster.

## Architecture

* **Supervisor**: Manages cluster topology, node registration, and peer notifications
* **Worker**: Executes tasks and connects to other nodes
* **Common**: Shared utilities and protocols

### Node Joining Process

1. **Initial Handshake**

   * Worker sends `NodeToServer::Hello`
   * Supervisor responds `ServerToNode::Welcome`

2. **Peer Discovery**

   * Supervisor sends `ServerToNode::PeerList`
   * Supervisor notifies existing nodes with `ServerToNode::PeerJoin`

3. **Peer Connection**

   * New node updates peer list and establishes connections
   * Existing nodes update temporary registry and await incoming connections

## Features

* Direct peer-to-peer communication between workers
* Supervisor-based cluster management
* Local and temporary registries for connection tracking
* Written in Rust for concurrency and safety

## Project Structure

```
distributed-nodes/
├── common/         # Shared types, protocols, utilities
├── supervisor/     # Supervisor implementation
├── worker/         # Worker implementation
├── Cargo.toml      # Workspace config
├── Cargo.lock      # Dependency lock
├── start.sh        # Launch script
└── readme.md       # Project description
```

## Getting Started

### Prerequisites

* Rust stable version
* Cargo

### Build

```bash
git clone https://github.com/Rayane-Aboud/distributed-nodes.git
cd distributed-nodes
cargo build --release
```

### Run

```bash
./start.sh
```

Or manually:

```bash
cargo run --bin supervisor
cargo run --bin worker
```

## Development Roadmap

* ✅ Supervisor-worker architecture
* ✅ Node registration & handshake
* ✅ Peer discovery
* ⏳ Connection management
* ⏳ Logging with condition variables

Planned:

* [ ] Full logging system
* [ ] Peer connection verification
* [ ] Fault tolerance
* [ ] Load balancing and task distribution

## License

Open source. See repository for details.

## Author

**Rayane Aboud** – Software & Systems Engineer, High-Performance & Distributed Systems, ESI Algiers.
