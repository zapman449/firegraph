#!/bin/bash

for node in 1 2 3 ; do
    for host in 0 1 2 3 4 5 6 ; do
        echo -n "beacon${node}x0${host} "
        ssh beacon${node}x0${host} tail -5 /tmp/beacon_client.log
    done
done
