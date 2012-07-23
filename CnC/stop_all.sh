#!/bin/bash

for node in 1 2 3 ; do
    for host in 0 1 2 3 4 5 6 ; do
        ssh beacon${node}x0${host} "/tmp/beacon_client.py stop" &
    done
done
