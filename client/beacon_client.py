#!/usr/bin/python

import os
import os.path
import socket
import sys
import time

DIR = '/logs/ns_logs/nginx'
DEST_IP = '3.4.163.195'
UDP_PORT = 45454

#referer=http://www.weather.com/weather/today/Kansas+City+MO+64152?lswe=64152&lwsa=WeatherLocalUndeclared&from=searchbox_localwx
#referer=http://www.weather.com/weather/map/interactive/34655

def follow(thefile) :
    thefile.seek(0,2)
    while True :
        line = thefile.readline()
        if not line :
            time.sleep(0.1)
            continue
        yield line.strip()

def zipfromline(line) :
    parts = line.split('^')
    #d = {}
    #for p in parts :
    #    idx = p.find('=')
    #    d[p[0:idx]] = p[idx+1:]
    r = parts[7]
    r = r[r.find('=')+1:]
    url_parts = r.split('/')
    if len(url_parts) < 6 :
        #print repr(url_parts)
        return None
    forecast = url_parts[4]
    location = url_parts[5]
    if location in ('graph', 'None', '') :
        return None
    if '?' in location :
        location = location[0:location.find('?')]
    #print 'returning', repr(location)
    return location

def main() :
    global DIR
    files = os.listdir(DIR)
    result = None
    for file in files :
        if file.startswith('beacon') :
            result = file
            break
    if not result :
        print 'mer?'
        sys.exit()
    log = open(os.path.join(DIR, file), 'r')
    lines = follow(log)
    lines2 = (line for line in lines if '/weather/' in line)
    lines3 = (line for line in lines2 if '/weather/map/' not in line)
    zips = (zipfromline(line) for line in lines3)
    counter = 0
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    try :
        for z in zips :
            if z == None :
                continue
            counter += 1
            #print z
            sock.sendto( z, 0, (DEST_IP, UDP_PORT))
    except KeyboardInterrupt :
        print "sent messages: %d" % counter
        raise

if __name__ == '__main__' :
    main()
