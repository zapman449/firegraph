#!/bin/bash

for node in 1 2 3 ; do
    for host in 0 1 2 3 4 5 6 ; do
        ssh beacon${node}x0${host} "nohup /tmp/beacon_client.py &" &
    done
done
