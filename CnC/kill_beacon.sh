#!/bin/bash

export PATH=/bin:/sbin:/usr/bin:/usr/sbin

ps -ef | grep beacon_client | awk '{print $2}' | xargs kill
