#!/bin/bash

set -e

KEY=../kv-node-key.pem
PORT=3030

PUBLIC_IP_FILE=../public_ips.txt
PRIVATE_IP_FILE=../private_ips.txt
SQS_FILE=../sqs_urls.txt
SNS_FILE=../sns_topic.txt
AWS_ENV_FILE=../aws.env

KILL_DURATION=30

# -------- DOCKER IMAGE --------
DOCKER_IMAGE="owais372k/kv-node:latest"

# -------- VALIDATION --------
[ $# -eq 0 ] && echo "Usage: ./chaos.sh ip1,ip2,ip3" && exit 1

[ ! -f "$KEY" ] && echo "Missing key" && exit 1
[ ! -f "$PUBLIC_IP_FILE" ] && echo "Missing public_ips.txt" && exit 1
[ ! -f "$PRIVATE_IP_FILE" ] && echo "Missing private_ips.txt" && exit 1
[ ! -f "$SQS_FILE" ] && echo "Missing sqs_urls.txt" && exit 1
[ ! -f "$SNS_FILE" ] && echo "Missing sns_topic.txt" && exit 1
[ ! -f "$AWS_ENV_FILE" ] && echo "Missing aws.env" && exit 1

# -------- LOAD AWS ENV --------
export $(grep -v '^#' "$AWS_ENV_FILE" | xargs)

# -------- READ FILES --------
PUBLIC_IPS=()
while IFS= read -r line || [ -n "$line" ]; do
  PUBLIC_IPS+=("$line")
done < "$PUBLIC_IP_FILE"

PRIVATE_IPS=()
while IFS= read -r line || [ -n "$line" ]; do
  PRIVATE_IPS+=("$line")
done < "$PRIVATE_IP_FILE"

SQS_URLS=()
while IFS= read -r line || [ -n "$line" ]; do
  SQS_URLS+=("$line")
done < "$SQS_FILE"

SNS_TOPIC_ARN=$(cat "$SNS_FILE")

# -------- BUILD NODE LIST --------
NODE_LIST=$(paste -sd "," "$PRIVATE_IP_FILE")

echo "Cluster: $NODE_LIST"

# -------- PARSE INPUT --------
IFS=',' read -r -a TARGET_IPS <<< "$1"

echo "========== CHAOS TEST =========="

for TARGET_IP in "${TARGET_IPS[@]}"; do

  TARGET_IP=$(echo "$TARGET_IP" | xargs)

  echo "================================="
  echo "💣 Target: $TARGET_IP"
  echo "================================="

  INDEX=-1

  for i in "${!PUBLIC_IPS[@]}"; do

    if [ "${PUBLIC_IPS[$i]}" == "$TARGET_IP" ]; then
      INDEX=$i
      break
    fi

  done

  if [ "$INDEX" -eq -1 ]; then
    echo "❌ IP not found: $TARGET_IP"
    continue
  fi

  PRIVATE_IP=${PRIVATE_IPS[$INDEX]}
  SQS_URL=${SQS_URLS[$INDEX]}

  echo "Private IP: $PRIVATE_IP"

  # -------- KILL NODE --------
  echo "💀 Killing node..."

  ssh \
    -o StrictHostKeyChecking=no \
    -i $KEY \
    ubuntu@$TARGET_IP << EOF

sudo docker rm -f kv-node || true

EOF

  echo "⏳ Node down for ${KILL_DURATION}s..."
  sleep $KILL_DURATION

  # -------- REVIVE NODE --------
  echo "🔄 Restarting node..."

  ssh \
    -o StrictHostKeyChecking=no \
    -i $KEY \
    ubuntu@$TARGET_IP << EOF

set -e

echo "========== RECOVERY =========="

sudo systemctl start docker

echo "Pulling latest image..."

sudo docker pull $DOCKER_IMAGE

echo "Creating persistence directory..."

sudo mkdir -p /home/ubuntu/kv-data

echo "Removing old container..."

sudo docker rm -f kv-node || true

echo "Starting container..."

sudo docker run -d \
  --name kv-node \
  --restart unless-stopped \
  -p $PORT:$PORT \
  -v /home/ubuntu/kv-data:/app/data \
  -e NODE_LIST="$NODE_LIST" \
  -e MY_IP="$PRIVATE_IP" \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  -e AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  -e AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
  -e SNS_TOPIC_ARN="$SNS_TOPIC_ARN" \
  -e SQS_QUEUE_URL="$SQS_URL" \
  $DOCKER_IMAGE

EOF

  echo "⏳ Waiting for recovery..."
  sleep 10

  # -------- VERIFY --------
  echo "🔍 Checking health..."

  if curl -s --max-time 5 http://$TARGET_IP:$PORT/health >/dev/null; then

    echo "✅ Node recovered successfully: $TARGET_IP"

  else

    echo "❌ Node recovery failed: $TARGET_IP"

    ssh \
      -o StrictHostKeyChecking=no \
      -i $KEY \
      ubuntu@$TARGET_IP << EOF

echo "========== DOCKER LOGS =========="

sudo docker logs kv-node || true

EOF

  fi

  echo ""

done

echo "🎯 Chaos test complete"