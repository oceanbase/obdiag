#!/bin/bash

PROCESS_NAME="obd"

while true; do
    # Check if the process is running
    if pgrep -x "$PROCESS_NAME" > /dev/null; then
        echo "$PROCESS_NAME is running. Checking again in 5 second..."
        sleep 5
    else
        echo "$PROCESS_NAME is not running. Exiting."
        exit 0
    fi
done