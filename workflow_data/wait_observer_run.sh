#!/bin/bash

SUCCESS_MSG="boot success!"
FAILURE_MSG="boot failed!"

LOG_FILE=$(mktemp)

function cleanup {
    kill "$LOG_PID" 2>/dev/null
    rm -f "$LOG_FILE"
}

trap cleanup EXIT

docker logs -f "$obdiag_ob" >"$LOG_FILE" 2>&1 &

LOG_PID=$!

while true; do
    while IFS= read -r line; do
        echo "$line"
        if [[ "$line" == *"$SUCCESS_MSG"* || "$line" == *"$FAILURE_MSG"* ]]; then
            cleanup
            exit 0
        fi
    done < <(tail -f "$LOG_FILE")
done