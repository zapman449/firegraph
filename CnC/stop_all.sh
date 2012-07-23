#!/bin/bash

touch 'stop'
/opt/TWC/bin/serverlist -perline | grep "^beacon" | while read server ; do
    scp 'stop' ${server}:/tmp
done
rm -f 'stop'

echo "--- sleeping ---"
sleep 5

/opt/TWC/bin/serverlist -perline | grep "^beacon" | while read server ; do
    scp -q ${server}:/tmp/stop .
    echo -n "$server  "
    cat stop
done
rm -f stop
