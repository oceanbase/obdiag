#!/bin/sh
while true; do
    ps -aux | grep obd | grep -v grep
    if [ $? -eq 0 ]; then
        echo "Process exists, checking again in 5 second..."
        sleep 5
    else
        echo "Process not found. Exiting."
        exit 0
    fi
done
