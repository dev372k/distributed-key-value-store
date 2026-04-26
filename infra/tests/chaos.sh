#!/bin/bash

KEY=../kv-node-key.pem

# Read IPs (macOS safe)
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
  # Pick 2 DIFFERENT nodes
  INDEX1=$((RANDOM % COUNT))
  INDEX2=$((RANDOM % COUNT))

  while [ "$INDEX1" -eq "$INDEX2" ]; do
    INDEX2=$((RANDOM % COUNT))
  done

  PUB1=${PUBLIC_IPS[$INDEX1]}
  PUB2=${PUBLIC_IPS[$INDEX2]}

  PRI1=${PRIVATE_IPS[$INDEX1]}
  PRI2=${PRIVATE_IPS[$INDEX2]}

  echo "Killing nodes:"
  echo " - $PUB1 ($PRI1)"
  echo " - $PUB2 ($PRI2)"

  # Kill both
  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUB1 "pkill kv_store || true"
  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUB2 "pkill kv_store || true"

  sleep 10

  echo "🔄 Restarting nodes..."

  # Build cluster list
  NODE_LIST=""
  for ip in "${PRIVATE_IPS[@]}"; do
    NODE_LIST+="http://$ip:3030 "
  done

  # Restart node 1
  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUB1 << EOF
    chmod +x ~/kv_store
    nohup ~/kv_store 3030 \
    http://$PRI1:3030 \
    $NODE_LIST \
    > log.txt 2>&1 &
EOF

  # Restart node 2
  ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$PUB2 << EOF
    chmod +x ~/kv_store
    nohup ~/kv_store 3030 \
    http://$PRI2:3030 \
    $NODE_LIST \
    > log.txt 2>&1 &
EOF

  sleep 5

  echo "Checking recovery..."

  ssh -i $KEY ubuntu@$PUB1 "ps aux | grep kv_store | grep -v grep || echo FAILED"
  ssh -i $KEY ubuntu@$PUB2 "ps aux | grep kv_store | grep -v grep || echo FAILED"

  echo "-----------------------------------"

  sleep 20
done