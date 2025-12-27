#!/bin/sh
# Compile entire workspace
cargo build --release

# Start supervisor
./target/release/supervisor &

# Allow server socket to bind
sleep 1

# Launch multiple independent workers
./target/release/worker worker-1 &
./target/release/worker worker-2 &
./target/release/worker worker-3 &

wait
