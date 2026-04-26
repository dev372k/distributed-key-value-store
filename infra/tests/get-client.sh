#!/bin/bash

# Parse CLI arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --key) KEY="$2"; shift ;;
    *) echo "Unknown parameter: $1"; exit 1 ;;
  esac
  shift
done

# Validate input (only key required)
if [[ -z "$KEY" ]]; then
  echo "Usage: $0 --key <key>"
  exit 1
fi

NODES=($(cat ../public_ips.txt))

for node in "${NODES[@]}"; do
  response=$(curl -s "http://$node:3030/get?key=$KEY")
  status=$?

  if [[ $status -eq 0 ]]; then
    echo "Success on $node"
    echo "Response: $response"
    break
  else
    echo "Failed on $node"
  fi
done