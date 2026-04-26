#!/bin/bash

# macOS-safe file reading
PUBLIC_IPS=()
while IFS= read -r line || [ -n "$line" ]; do
  PUBLIC_IPS+=("$line")
done < ../public_ips.txt

PRIVATE_IPS=()
while IFS= read -r line || [ -n "$line" ]; do
  PRIVATE_IPS+=("$line")
done < ../private_ips.txt

COUNT=${#PUBLIC_IPS[@]}

while true; do
  INDEX=$((RANDOM % COUNT))

  PUBLIC_IP=${PUBLIC_IPS[$INDEX]}
  PRIVATE_IP=${PRIVATE_IPS[$INDEX]}

  echo "Stopping node: $PUBLIC_IP ($PRIVATE_IP)"

  ssh -o StrictHostKeyChecking=no ubuntu@$PUBLIC_IP << EOF
    pkill kv_store || true
EOF

  sleep 10

  echo "Restarting node: $PUBLIC_IP"

  NODE_LIST=""
  for ip in "${PRIVATE_IPS[@]}"; do
    NODE_LIST+="http://$ip:3030 "
  done

  ssh -o StrictHostKeyChecking=no ubuntu@$PUBLIC_IP << EOF
    chmod +x ~/kv_store

    nohup ~/kv_store 3030 \
    http://$PRIVATE_IP:3030 \
    $NODE_LIST \
    > log.txt 2>&1 &

    sleep 2
    ps aux | grep kv_store | grep -v grep || echo "FAILED TO RESTART"
EOF

  sleep 20
done