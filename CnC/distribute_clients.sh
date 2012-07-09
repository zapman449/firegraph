#!/bin/bash

/opt/TWC/bin/serverlist -perline | grep "^beacon" | while read server ; do
    scp beacon_client.py ${server}:/tmp
done
