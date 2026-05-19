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

# =========================================================
# CONSISTENT HASHING
# =========================================================

hash_key() {
  local key=$1
  echo -n "$key" | sha1sum | awk '{print $1}'
}

declare -a RING_HASHES
declare -a RING_NODES

build_ring() {

  for node in "${NODES[@]}"; do

    HASH=$(hash_key "$node")

    RING_HASHES+=("$HASH")
    RING_NODES+=("$node")

  done

  # sort ring
  for ((i=0; i<${#RING_HASHES[@]}; i++)); do

    for ((j=i+1; j<${#RING_HASHES[@]}; j++)); do

      if [[ "${RING_HASHES[$i]}" > "${RING_HASHES[$j]}" ]]; then

        TMP_HASH=${RING_HASHES[$i]}
        RING_HASHES[$i]=${RING_HASHES[$j]}
        RING_HASHES[$j]=$TMP_HASH

        TMP_NODE=${RING_NODES[$i]}
        RING_NODES[$i]=${RING_NODES[$j]}
        RING_NODES[$j]=$TMP_NODE

      fi
    done
  done
}

build_ring

get_primary_node() {

  local key=$1

  KEY_HASH=$(hash_key "$key")

  for ((i=0; i<${#RING_HASHES[@]}; i++)); do

    if [[ "$KEY_HASH" < "${RING_HASHES[$i]}" ]]; then
      echo "${RING_NODES[$i]}"
      return
    fi
  done

  # wrap around
  echo "${RING_NODES[0]}"
}

get_replica_nodes() {

  local primary=$1
  local replicas=()

  INDEX=-1

  for ((i=0; i<COUNT; i++)); do

    if [[ "${RING_NODES[$i]}" == "$primary" ]]; then
      INDEX=$i
      break
    fi
  done

  if [[ $INDEX -eq -1 ]]; then
    return
  fi

  for ((i=0; i<3; i++)); do

    IDX=$(( (INDEX + i) % COUNT ))

    replicas+=("${RING_NODES[$IDX]}")
  done

  echo "${replicas[@]}"
}

# =========================================================
# COMMANDS
# =========================================================

CMD=$1
shift

case "$CMD" in

# =========================================================
# PUT
# =========================================================

put)

  KEY=$1
  VALUE=$2

  if [[ -z "$KEY" || -z "$VALUE" ]]; then
    echo "Usage: ./kv.sh put <key> <value>"
    exit 1
  fi

  TARGET_NODE=$(get_primary_node "$KEY")

  echo "Target node: $TARGET_NODE"

  curl -s \
    "http://$TARGET_NODE:3030/put?key=$KEY&value=$VALUE"

  echo ""
  ;;

# =========================================================
# GET
# =========================================================

get)

  KEY=$1

  if [[ -z "$KEY" ]]; then
    echo "Usage: ./kv.sh get <key>"
    exit 1
  fi

  PRIMARY=$(get_primary_node "$KEY")

  echo "Primary node: $PRIMARY"

  response=$(curl -s --max-time 2 \
    "http://$PRIMARY:3030/get?key=$KEY")

  if [[ $? -eq 0 && -n "$response" ]]; then
    echo "$response"
    exit 0
  fi

  echo "Primary failed, trying replicas..."

  REPLICAS=($(get_replica_nodes "$PRIMARY"))

  for NODE in "${REPLICAS[@]}"; do

    if [[ "$NODE" == "$PRIMARY" ]]; then
      continue
    fi

    response=$(curl -s --max-time 2 \
      "http://$NODE:3030/get?key=$KEY")

    if [[ $? -eq 0 && -n "$response" ]]; then

      echo "Served by replica: $NODE"
      echo "$response"

      exit 0
    fi
  done

  echo "All replicas failed"
  exit 1
  ;;

# =========================================================
# STATS
# =========================================================

stats)

  echo "========== CLUSTER METRICS =========="

  for node in "${NODES[@]}"; do

    echo ""
    echo "Node: $node"

    curl -s \
      "http://$node:3030/metrics"

    echo ""
  done
  ;;

# =========================================================
# NODES
# =========================================================

nodes)

  echo "========== CLUSTER NODES =========="

  for node in "${NODES[@]}"; do
    echo "$node"
  done
  ;;

# =========================================================
# RING
# =========================================================

ring)

  echo "========== CONSISTENT HASH RING =========="

  for ((i=0; i<${#RING_HASHES[@]}; i++)); do

    echo "${RING_HASHES[$i]} -> ${RING_NODES[$i]}"
  done
  ;;

# =========================================================
# BENCHMARK
# =========================================================

bench)

  TOTAL=$1

  if [[ -z "$TOTAL" ]]; then
    TOTAL=100
  fi

  TOTAL=$(echo "$TOTAL" | tr -d '[]')

  if ! [[ "$TOTAL" =~ ^[0-9]+$ ]]; then
    echo "Usage: ./kv.sh bench <num_requests>"
    exit 1
  fi

  echo "Running benchmark with $TOTAL requests..."

  START=$(date +%s)

  for ((i=0; i<TOTAL; i++)); do

    KEY="bench_$i"
    VALUE=$i

    NODE=$(get_primary_node "$KEY")

    curl -s \
      "http://$NODE:3030/put?key=$KEY&value=$VALUE" \
      >/dev/null &
  done

  wait

  END=$(date +%s)

  DURATION=$((END - START))

  [[ $DURATION -eq 0 ]] && DURATION=1

  echo ""
  echo "========== BENCHMARK RESULT =========="
  echo "Requests: $TOTAL"
  echo "Duration: $DURATION sec"
  echo "Throughput: $((TOTAL / DURATION)) ops/sec"
  ;;

# =========================================================
# HELP
# =========================================================

help)

  echo "Available commands:"
  echo ""
  echo "  ./kv.sh put <key> <value>"
  echo "  ./kv.sh get <key>"
  echo "  ./kv.sh stats"
  echo "  ./kv.sh nodes"
  echo "  ./kv.sh ring"
  echo "  ./kv.sh bench [num_requests]"
  ;;

# =========================================================
# DEFAULT
# =========================================================

*)

  echo "Unknown command: $CMD"
  echo ""

  echo "Available commands:"
  echo "  ./kv.sh put <key> <value>"
  echo "  ./kv.sh get <key>"
  echo "  ./kv.sh stats"
  echo "  ./kv.sh nodes"
  echo "  ./kv.sh ring"
  echo "  ./kv.sh bench [num_requests]"

  exit 1
  ;;
esac