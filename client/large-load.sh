#!/bin/bash

TOTAL_KEYS=60000
VALUE_SIZE=10240

VALUE=$(head -c $VALUE_SIZE < /dev/zero | tr '\0' 'A')

START=$(date +%s)

export VALUE

seq 1 $TOTAL_KEYS | xargs -P 200 -I {} bash -c '
./kv.sh put key{} "$VALUE" >/dev/null
'

END=$(date +%s)

echo ""
echo "Completed."
echo "Duration: $((END - START)) seconds"