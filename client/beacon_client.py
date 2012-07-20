#!/usr/bin/python

import cPickle as pickle
import logging
import logging.handlers
import os
import os.path
import re
import socket
from stat import ST_INO, ST_DEV
import sys
import time

DEBUG = True

BEACONDIR = '/logs/ns_logs/nginx'
HOMEDIR = '/tmp'
STOP_FILE = os.path.join(HOMEDIR, 'stop')
JOIN_STR = '^'
DEST_IP = '3.4.163.195'
UDP_PORT = 45454
PICKLE = os.path.join(HOMEDIR, 'loc.pickle')
LOGFILE = os.path.join(HOMEDIR, 'beacon_client.log')
#referer=http://www.weather.com/weather/today/Kansas+City+MO+64152?lswe=64152&lwsa=WeatherLocalUndeclared&from=searchbox_localwx
#referer=http://www.weather.com/weather/map/interactive/34655

def build_logger() :
    """ build my custom logger. Log to file by default. Criticals go to console.
    Rotate logfiles after 1mb"""
    global LOGFILE
    global DEBUG
    log = logging.getLogger()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    hostname = os.uname()[1]
    if '.' in hostname :
        hostname = hostname[0:hostname.find('.')]
    format_str = "%s %%(levelname)s\t: %%(message)s" % hostname
    console_format = logging.Formatter(format_str)
    console_handler.setFormatter(console_format)
    file_handler = logging.handlers.RotatingFileHandler(LOGFILE, maxBytes=1000000,
            backupCount=6)
    if DEBUG :
        log.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
    else :
        log.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    # had to remove $(funcName)s from format line because it's not in python 2.4.x
    file_handler.setFormatter(file_format)
    log.addHandler(console_handler)
    log.addHandler(file_handler)
    return log

class LogTail:
    """A generator class to yield whole lines from a given file.  Designed to be
    robust in the face of file rotations on UNIXish platforms. (stole the logic for 
    this from the 'WatchedFileHandler' in logging.handler)"""
    def __init__(self, logfile, logger) :
        self.logger = logger
        self.logfile = os.path.abspath(logfile)
        stat = os.stat(self.logfile)
        self.dev, self.inode = stat[ST_DEV], stat[ST_INO]
        self.f = open(self.logfile, 'r')
        self.f.seek(0,2)
        # go to the end of the file on startup.
    def _reset(self) :
        self.logger.info('found rotated file. resetting')
        self.f.close()
        self.f = open(self.logfile, 'r')
        stat = os.stat(self.logfile)
        self.dev, self.inode = stat[ST_DEV], stat[ST_INO]
    def tail(self) :
        counter = 0
        while True :
            line = self.f.readline()
            if line :
                yield line.strip()
                counter = 0
            else :
                counter += 1
                if counter == 30 :
                    self.logger.info('3 seconds without a beacon. yielding None')
                    yield None
                    counter = 0
                stat = os.stat(self.logfile)
                tdev, tinode = stat[ST_DEV], stat[ST_INO]
                if tdev == self.dev and tinode == self.inode :
                    time.sleep(0.1)
                else :
                    self._reset()

def locfromline(line) :
    if line == None :
        return None
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
    #forecast = url_parts[4]
    location = url_parts[5]
    if location in ('graph', 'None', '') :
        return None
    if '?' in location :
        location = location[0:location.find('?')]
    #print 'returning', repr(location)
    return location

def latlongfromloc(locstr) :
    if locstr == None :
        return None,None
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

def main(logger) :
    global STOP_FILE
    global BEACONDIR
    global JOIN_STR
    if os.path.isfile(STOP_FILE) :
        logger.debug('removing preexisting STOP_FILE %s' % STOP_FILE)
        os.unlink(STOP_FILE)
    files = os.listdir(BEACONDIR)
    result = None
    for file in files :
        if file.startswith('beacon') :
            result = file
            break
    if not result :
        logger.critical('failed to find beacon log file. Exiting.')
        sys.exit()
    beacon_log = os.path.join(BEACONDIR, file)
    #lines = follow(beacon_log)
    #lines2 = (line for line in lines if '/weather/' in line)
    lines = LogTail(beacon_log, logger)
    lines2 = (line for line in lines.tail() if line == None or '/weather/' in line)
    lines3 = (line for line in lines2 if line == None or '/weather/map/' not in line)
    lines4 = (line for line in lines3 if line == None or '/b/impression' in line)
    lines5 = (line for line in lines4 if line == None or 'tile=1&' in line)
    locs = (locfromline(line) for line in lines5)
    latlong = ( latlongfromloc(loc) for loc in locs if loc != None )
    counter = 0
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    try :
        for lat,longi in latlong :
            counter += 1
            if counter % 50 == 0 :
                if os.path.isfile(STOP_FILE) :
                    break
            if lat == None :
                continue
            #print lat,longi
            lat = "%3.1f" % float(lat)
            longi = "%3.1f" % float(longi)
            #print lat,longi
            sock.sendto( JOIN_STR.join((lat, longi)), 0, (DEST_IP, UDP_PORT))
    except KeyboardInterrupt :
        print "sent messages: %d" % counter
        raise
    s = open(STOP_FILE, 'w')
    s.write("sent messages: %d\n" % counter)

if __name__ == '__main__' :
    logger = build_logger()
    logger.debug('opening pickled location info')
    p = open(PICKLE, 'rb')
    locdict = pickle.load(p)
    p.close()
    try :
        main(logger)
    except :
        logger.exception('error running main()')
        raise
