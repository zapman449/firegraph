firegraph
=========

Code to make a heatmap of live users on the website

Status 11:24am Monday:
beacon_client seems to work fine.  Forwards all location info to the server.
Note: this is designed to have as little impact on the beacon boxes as
possible.  

server.py: works in a basic way.  Throwing away all data, just keeping counts
from the beacons currently.  Lame, but it's a step in the right direction.

The various .sh scripts are command and controll code, designed to be run from
dogg.  they do what you'd expect from the names.

Next ToDos:
figure out data storage, snapshots, and how to present the data. No biggie. ;-)

Also: make the clients resiliant to log-rollover
