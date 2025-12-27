#!/bin/sh
set -e

pids=""

cleanup() {
    echo "Stopping all nodes"
    kill $pids 2>/dev/null
    exit 0
}

trap cleanup INT TERM

cargo build --release

./target/release/supervisor &
pids="$pids $!"

sleep 1

./target/release/worker worker-1 &
pids="$pids $!"

./target/release/worker worker-2 &
pids="$pids $!"

./target/release/worker worker-3 &
pids="$pids $!"

wait
