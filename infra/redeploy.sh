#!/bin/bash

KEY="kv-node-key.pem"

PUBLIC_IPS=($(cat public_ips.txt))
PRIVATE_IPS=($(cat private_ips.txt))

N=${#PUBLIC_IPS[@]}

NODE_LIST=""
for ((i=0; i<N; i++)); do
  NODE_LIST="$NODE_LIST http://${PRIVATE_IPS[$i]}:3030"
done

echo "Cluster nodes:"
echo $NODE_LIST

for ((i=0; i<N; i++)); do
  PUBLIC=${PUBLIC_IPS[$i]}
  PRIVATE=${PRIVATE_IPS[$i]}

  echo "⚡ Updating node $i ($PUBLIC)"

  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUBLIC "
    set -e

    source \$HOME/.cargo/env

    cd distributed-key-value-store

    git pull

    cargo build --release

    pkill kv_store || true

    nohup ./target/release/kv_store 3030 $NODE_LIST > log.txt 2>&1 &
  " &

done

wait
echo "⚡ Cluster updated successfully!"