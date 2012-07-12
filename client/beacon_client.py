#!/usr/bin/python

import cPickle as pickle
import os
import os.path
import re
import socket
from stat import ST_SIZE
import sys
import time

DIR = '/logs/ns_logs/nginx'
STOP_FILE = '/tmp/stop'
JOIN_STR = '^'
DEST_IP = '3.4.163.195'
UDP_PORT = 45454
PICKLE = '/tmp/loc.pickle'
#referer=http://www.weather.com/weather/today/Kansas+City+MO+64152?lswe=64152&lwsa=WeatherLocalUndeclared&from=searchbox_localwx
#referer=http://www.weather.com/weather/map/interactive/34655

def follow(thefile) :
    log = open(thefile, 'r')
    log.seek(0,2)
    counter = 0
    while True :
        line = log.readline()
        if not line :
            print 'sleeping', counter
            time.sleep(0.1)
            counter += 1
            if counter > 20 :
                # if we get to 2 seconds without a line, see if we need to reset
                counter = 0
                cur_size = os.stat(thefile)[ST_SIZE]
                cur_pos = log.tell()
                #print 'size', cur_size, 'pos', cur_pos
                if cur_size < cur_pos :
                    # if the size of the file is less than the position in the
                    # file...
                    log.close()
                    log = open(thefile, 'r')
            continue
        #print '=> yielding'
        yield line.strip()

def locfromline(line) :
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

def latlongfromloc(locstr) :
    #re_zip = re.compile('\d{5}')
    #re_loc = re.compile('US[A-Z]{2}\d{4}')
    re_loc = re.compile('\d{5}|US[A-Z]{2}/d{4}')
    # match either 5 digits (zip code) or US Weather Code (US<State><4digits>)
    lsearch = re_loc.search(locstr)
    if lsearch == None :
        return None,None
    else :
        zip_or_wcode = lsearch.group()
        try :
            return locdict[zip_or_wcode]
        except KeyError :
            return None,None

def main() :
    global STOP_FILE
    global DIR
    global JOIN_STR
    if os.path.isfile(STOP_FILE) :
        os.unlink(STOP_FILE)
    files = os.listdir(DIR)
    result = None
    for file in files :
        if file.startswith('beacon') :
            result = file
            break
    if not result :
        print 'mer?'
        sys.exit()
    log = os.path.join(DIR, file)
    lines = follow(log)
    lines2 = (line for line in lines if '/weather/' in line)
    lines3 = (line for line in lines2 if '/weather/map/' not in line)
    lines4 = (line for line in lines3 if '/b/impression' not in line)
    lines5 = (line for line in lines4 if 'tile=1&' not in line)
    locs = (locfromline(line) for line in lines5)
    latlong = ( latlongfromloc(loc) for loc in locs if loc != None )
    counter = 0
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    try :
        for lat,longi in latlong :
            if lat == None :
                continue
            counter += 1
            #print lat,longi
            lat = "%3.1f" % float(lat)
            longi = "%3.1f" % float(longi)
            #print lat,longi
            sock.sendto( JOIN_STR.join((lat, longi)), 0, (DEST_IP, UDP_PORT))
            if counter % 50 == 0 :
                if os.path.isfile(STOP_FILE) :
                    break
    except KeyboardInterrupt :
        print "sent messages: %d" % counter
        raise
    s = open(STOP_FILE, 'w')
    s.write("sent messages: %d\n" % counter)

if __name__ == '__main__' :
    p = open(PICKLE, 'rb')
    locdict = pickle.load(p)
    p.close()
    main()
