#!/bin/bash

# Parse CLI arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --key) KEY="$2"; shift ;;
    --value) VALUE="$2"; shift ;;
    *) echo "Unknown parameter: $1"; exit 1 ;;
  esac
  shift
done

# Validate input
if [[ -z "$KEY" || -z "$VALUE" ]]; then
  echo "Usage: $0 --key <key> --value <value>"
  exit 1
fi

NODES=($(cat ../public_ips.txt))

for node in "${NODES[@]}"; do
  response=$(curl -s "http://$node:3030/put?key=$KEY&value=$VALUE")
  status=$?

  if [[ $status -eq 0 ]]; then
    echo "Success on $node"
    echo "Response: $response"
    break
  else
    echo "Failed on $node"
  fi
done