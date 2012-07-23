#!/bin/bash

# to be copied to the beacon servers.  Run by force_kill.

export PATH=/bin:/sbin:/usr/bin:/usr/sbin

ps -ef | grep beacon_client | awk '{print $2}' | xargs kill
