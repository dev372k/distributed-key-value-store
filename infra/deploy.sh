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

  echo "Deploying node $i ($PUBLIC)"

  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUBLIC "
    set -e

    sudo apt update -y
    sudo apt install -y git curl build-essential pkg-config libssl-dev

    if [ ! -d \"\$HOME/.cargo\" ]; then
      curl https://sh.rustup.rs -sSf | sh -s -- -y
    fi

    source \$HOME/.cargo/env

    if [ ! -d distributed-key-value-store ]; then
      git clone https://github.com/dev372k/distributed-key-value-store.git
    fi

    cd distributed-key-value-store
    git pull

    cargo build --release

    pkill kv_store || true

    nohup ./target/release/kv_store 3030 $NODE_LIST > log.txt 2>&1 &
  " &

done

wait
echo "Cluster deployed successfully!"