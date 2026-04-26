#!/bin/bash

set -e

echo "========== SETUP =========="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Project root: $PROJECT_ROOT"

# -------- CONFIG --------
BIN_PATH="$PROJECT_ROOT/build-output/kv_store"
PUBLIC_IP_FILE="$SCRIPT_DIR/public_ips.txt"
PRIVATE_IP_FILE="$SCRIPT_DIR/private_ips.txt"
KEY_PATH="$SCRIPT_DIR/kv-node-key.pem"
PORT=3030
# ------------------------

# -------- VALIDATION --------
if [ ! -f "$PUBLIC_IP_FILE" ] || [ ! -f "$PRIVATE_IP_FILE" ]; then
  echo "❌ Missing IP files"
  exit 1
fi

if [ ! -f "$KEY_PATH" ]; then
  echo "❌ Missing SSH key"
  exit 1
fi

echo "========== BUILDING BINARY =========="

mkdir -p build-output

docker build --platform linux/amd64 -t kv-builder .

echo "========== EXTRACTING BINARY =========="

docker rm -f temp-kv 2>/dev/null || true
docker create --name temp-kv kv-builder > /dev/null
docker cp temp-kv:/output/kv_store "$BIN_PATH"
docker rm temp-kv > /dev/null

chmod +x "$BIN_PATH"

echo "Checking binary..."
file "$BIN_PATH"

# -------- READ IPS --------
PUBLIC_IPS=()
while IFS= read -r line || [ -n "$line" ]; do
  PUBLIC_IPS+=("$line")
done < "$PUBLIC_IP_FILE"

PRIVATE_IPS=()
while IFS= read -r line || [ -n "$line" ]; do
  PRIVATE_IPS+=("$line")
done < "$PRIVATE_IP_FILE"

# Remove duplicates
UNIQUE_IPS=($(printf "%s\n" "${PRIVATE_IPS[@]}" | sort -u))

echo "========== CLUSTER =========="

NODE_LIST=""
for ip in "${UNIQUE_IPS[@]}"; do
  NODE_LIST+="http://$ip:$PORT "
done

echo "$NODE_LIST"

echo "========== DEPLOY =========="

for i in "${!PUBLIC_IPS[@]}"; do
(
  PUBLIC_IP=${PUBLIC_IPS[$i]}
  PRIVATE_IP=${PRIVATE_IPS[$i]}

  echo "---- $PUBLIC_IP ----"

  # Upload
  scp -o StrictHostKeyChecking=no \
      -i "$KEY_PATH" \
      "$BIN_PATH" ubuntu@$PUBLIC_IP:/tmp/kv_store

  # Prepare args locally (CRITICAL FIX)
  ARGS="$PORT http://$PRIVATE_IP:$PORT $NODE_LIST"

  ssh -o StrictHostKeyChecking=no \
      -i "$KEY_PATH" ubuntu@$PUBLIC_IP << EOF

echo "========== REMOTE =========="

# Ensure binary exists
if [ ! -f /tmp/kv_store ]; then
  echo "❌ Upload failed"
  exit 1
fi

rm -f ~/kv_store
mv /tmp/kv_store ~/kv_store
chmod +x ~/kv_store

echo "Killing old processes..."
pkill -9 kv_store || true
fuser -k $PORT/tcp || true
sleep 2

echo "ARGS:"
echo "$ARGS"

echo "Starting node..."

nohup ~/kv_store $ARGS > log.txt 2>&1 &

sleep 3

echo "Checking..."

if ps aux | grep kv_store | grep -v grep > /dev/null; then
    echo "✅ RUNNING"
else
    echo "❌ FAILED"
    echo "---- LOG ----"
    cat log.txt || echo "No logs"
    echo "-------------"
fi

EOF

) &
done

wait

echo "========== DONE =========="