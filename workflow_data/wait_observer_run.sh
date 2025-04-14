#!/bin/sh
while true; do
    obclient -h127.0.0.1 -P2881 -uroot -Doceanbase -e "show databases;"
    if [ $? -eq 0 ]; then
        echo "Process not found. Exiting."
        exit 0
    else
        echo "Process exists, checking again in 5 second..."
        sleep 5
    fi
done
