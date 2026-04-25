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
  SELF="http://${PRIVATE}:3030"

  echo "Deploying node $i ($PUBLIC)"

  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUBLIC << EOF
set -e

sudo apt update -y
sudo apt install -y git curl build-essential pkg-config libssl-dev

# install rust if not exists
if [ ! -d "/home/ubuntu/.cargo" ]; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi

/home/ubuntu/.cargo/bin/cargo --version || { echo "Cargo install failed"; exit 1; }

rm -rf distributed-key-value-store
git clone https://github.com/dev372k/distributed-key-value-store.git

cd distributed-key-value-store

/home/ubuntu/.cargo/bin/cargo build --release

pkill kv_store || true

nohup ./target/release/kv_store 3030 $SELF $NODE_LIST > log.txt 2>&1 &

sleep 2
echo "===== NODE LOG ====="
cat log.txt
EOF

done

echo "Cluster deployed successfully!"