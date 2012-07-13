#!/usr/bin/python

import csv
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
    myfile = sys.argv[1]
    if not os.path.isfile(myfile) :
        print 'uh, like, file not found, dude.'
        sys.exit()
    baseTS = 0
    30min = 1800    # 30 min * 60 sec
    aggregate = {}
    for l in open(myfile) :
        if '/weather/' not in line :
            continue
        if '/weather/map' in line :
            continue
        if '/b/impression' not in line :
            continue
        ts,refer = line.split(',', 1)
        lat,longi = latlongfromloc(locfromline(refer))
        if lat == None :
            continue
        lat = "%3.1f" % float(lat)
        longi = "%3.1f" % float(longi)
        if ts > baseTS + 30min :
            baseTS = ts
            try :
                out.write('''"Latitude","Longitude","value"\n''')
                for lat, longi in aggregate :
                    out.write('''"%3.1f","%3.1f","%d"\n''' % (lat, longi, 
                                                  aggregate[(lat, longi)]))
                out.close()
                aggregate = {}
            except NameError :
                pass
            out = open('output.%d' % baseTS, 'w')
        try :
            aggregate[(lat,longi)] += 1
        except KeyError :
            aggregate[(lat,longi)] = 1

if __name__ == '__main__' :
    p = open(PICKLE, 'rb')
    locdict = pickle.load(p)
    p.close()
    main()
