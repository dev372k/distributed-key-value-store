#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IP_FILE="$SCRIPT_DIR/../infra/public_ips.txt"

# -------- VALIDATION --------
if [[ ! -f "$IP_FILE" ]]; then
  echo "Error: public_ips.txt not found at $IP_FILE"
  exit 1
fi

# -------- LOAD NODES --------
NODES=()
while IFS= read -r line || [ -n "$line" ]; do
  NODES+=("$line")
done < "$IP_FILE"

COUNT=${#NODES[@]}

if [[ $COUNT -eq 0 ]]; then
  echo "Error: No nodes found"
  exit 1
fi

# -------- HASH FUNCTION --------
hash_key() {
  local key=$1
  local hash=$(echo -n "$key" | sha1sum | awk '{print $1}')
  local num=$((16#${hash:0:8}))
  echo $((num % COUNT))
}

# -------- COMMAND --------
CMD=$1
shift

case "$CMD" in

# ================= PUT =================
put)
  KEY=$1
  VALUE=$2

  if [[ -z "$KEY" || -z "$VALUE" ]]; then
    echo "Usage: kv put <key> <value>"
    exit 1
  fi

  INDEX=$(hash_key "$KEY")
  TARGET_NODE=${NODES[$INDEX]}

  echo "Target node: $TARGET_NODE"

  curl -s "http://$TARGET_NODE:3030/put?key=$KEY&value=$VALUE"
  ;;

# ================= GET =================
get)
  KEY=$1

  if [[ -z "$KEY" ]]; then
    echo "Usage: kv get <key>"
    exit 1
  fi

  INDEX=$(hash_key "$KEY")
  PRIMARY=${NODES[$INDEX]}

  echo "Primary node: $PRIMARY"

  response=$(curl -s --max-time 2 "http://$PRIMARY:3030/get?key=$KEY")

  if [[ $? -eq 0 && -n "$response" ]]; then
    echo "$response"
    exit 0
  fi

  echo "Primary failed, trying replicas..."

  for ((i=1; i<COUNT; i++)); do
    NODE=${NODES[$(( (INDEX + i) % COUNT ))]}

    response=$(curl -s --max-time 2 "http://$NODE:3030/get?key=$KEY")

    if [[ $? -eq 0 && -n "$response" ]]; then
      echo "Served by replica: $NODE"
      echo "$response"
      exit 0
    fi
  done

  echo "All nodes failed"
  exit 1
  ;;

# ================= STATS =================
stats)
  echo "Cluster Metrics:"
  echo "----------------"

  for node in "${NODES[@]}"; do
    echo "Node: $node"
    curl -s "http://$node:3030/metrics"
    echo ""
  done
  ;;

# ================= NODES =================
nodes)
  echo "Cluster Nodes:"
  echo "--------------"
  for node in "${NODES[@]}"; do
    echo "$node"
  done
  ;;

# ================= BENCH =================
bench)
  TOTAL=$1

  if [[ -z "$TOTAL" ]]; then
    TOTAL=100
  fi

  TOTAL=$(echo "$TOTAL" | tr -d '[]')

  if ! [[ "$TOTAL" =~ ^[0-9]+$ ]]; then
    if [[ "$2" =~ ^[0-9]+$ ]]; then
      TOTAL=$2
    fi
  fi

  if ! [[ "$TOTAL" =~ ^[0-9]+$ ]]; then
    echo "Error: bench requires a number"
    echo "Usage: ./kv.sh bench 100"
    exit 1
  fi

  echo "Running benchmark with $TOTAL requests..."

  START=$(date +%s)

  for ((i=0; i<TOTAL; i++)); do
    KEY="bench_$i"
    VALUE=$i
    INDEX=$(hash_key "$KEY")
    NODE=${NODES[$INDEX]}

    curl -s "http://$NODE:3030/put?key=$KEY&value=$VALUE" >/dev/null &
  done

  wait

  END=$(date +%s)
  DURATION=$((END - START))

  [[ $DURATION -eq 0 ]] && DURATION=1

  echo "Completed in $DURATION seconds"
  echo "Throughput: $((TOTAL / DURATION)) ops/sec"
  ;;
# ================= HELP =================
help)
  echo "Available commands:"
  echo "  ./kv.sh put <key> <value>"
  echo "  ./kv.sh get <key>"
  echo "  ./kv.sh stats"
  echo "  ./kv.sh nodes"
  echo "  ./kv.sh bench [num_requests]"
  ;;

# ================= DEFAULT =================
*)
  echo "Unknown command: $CMD"
  echo ""
  echo "Available commands:"
  echo "  kv put <key> <value>"
  echo "  kv get <key>"
  echo "  kv stats"
  echo "  kv nodes"
  echo "  kv bench [num_requests]"
  exit 1
  ;;
esac