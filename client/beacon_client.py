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

import daemon

DEBUG = True

BEACONDIR = '/logs/ns_logs/nginx'
DATADIR = '/tmp'        # location of pickle, scripts, etc.
PIDDIR = '/tmp'
LOGDIR = '/tmp'
#DATADIR = '/web/firegraph/ # location of pickle, scripts, etc
#PIDDIR = '/var/run'     # where the beacon_client.pid file lives
#LOGDIR = '/var/log'     # where the beacon_client.log files live
JOIN_STR = '^'
DEST_IP = '3.4.163.195'
UDP_PORT = 45454
PICKLE = os.path.join(DATADIR, 'loc.pickle')
#LOGFILE = os.path.join(LOGDIR, 'beacon_client.log')
#referer=http://www.weather.com/weather/today/Kansas+City+MO+64152?lswe=64152&lwsa=WeatherLocalUndeclared&from=searchbox_localwx
#referer=http://www.weather.com/weather/map/interactive/34655

def build_logger() :
    """ build my custom logger. Log to file by default. Criticals go to console.
    Rotate logfiles after 1mb"""
    #global LOGFILE
    global DEBUG
    log = logging.getLogger()
    #console_handler = logging.StreamHandler()
    #console_handler.setLevel(logging.ERROR)
    hostname = os.uname()[1]
    if '.' in hostname :
        hostname = hostname[0:hostname.find('.')]
    #format_str = "%s %%(levelname)s\t: %%(message)s" % hostname
    #console_format = logging.Formatter(format_str)
    #console_handler.setFormatter(console_format)
    #file_handler = logging.handlers.RotatingFileHandler(LOGFILE, maxBytes=1000000,
    #        backupCount=6)
    file_handler = logging.handlers.SysLogHandler(address = '/dev/log')
    if DEBUG :
        log.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
    else :
        log.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
    #file_format = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_format = logging.Formatter("%(levelname)s %(message)s")
    # had to remove $(funcName)s from format line because it's not in python 2.4.x
    file_handler.setFormatter(file_format)
    #log.addHandler(console_handler)
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
    def _reset(self, fromthetop=True) :
        self.logger.info('found rotated file. resetting')
        self.f.close()
        while not os.path.exists(self.logfile) :
            self.logger.info('sleeping 5 seconds waiting on beacon log creation')
            time.sleep(5)
        self.f = open(self.logfile, 'r')
        if not fromthetop :
            self.f.seek(0,2)
        stat = os.stat(self.logfile)
        self.dev, self.inode = stat[ST_DEV], stat[ST_INO]
    def tail(self) :
        counter = 0
        errcounter = 0
        while True :
            line = self.f.readline()
            if line :
                yield line.strip()
                counter = 0
                errcounter = 0
            else :
                counter += 1
                time.sleep(0.1)
                if counter >= 30 :
                    self.logger.debug('3 seconds without a beacon. yielding None')
                    yield None
                    counter = 0
                    errcounter += 1
                    if errcounter >= 20 :
                        self.logger.info('forcing _reset due to 1 min without line')
                        self._reset(fromthetop=False)
                        errcounter = 0
                        continue
                    try :
                        stat = os.stat(self.logfile)
                    except IOError :
                        self.logger.info('forcing _reset due to IOError for stat(beacon log)')
                        self._reset()
                        continue
                    tdev, tinode = stat[ST_DEV], stat[ST_INO]
                    if tdev == self.dev and tinode == self.inode :
                        pass
                    else :
                        self.logger.info('forcing _reset due to dev/inode being different')
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

def latlongfromloc(locstr, locdict) :
    if locstr == None :
        return None,None
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
    global BEACONDIR
    global JOIN_STR
    locdict = open_pickle(logger)
    logger.debug('successfully opened pickled location info')
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
    latlong = ( latlongfromloc(loc, locdict) for loc in locs if loc != None )
    counter = 0
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    logger.info('starting central loop')
    try :
        for lat,longi in latlong :
            counter += 1
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
    except :
        logger.exception('wtf?')
        raise

def open_pickle(logger) :
    global PICKLE
    if os.path.exists(PICKLE) :
        p = open(PICKLE, 'rb')
    else :
        logger.critical('failed to find pickled location info!')
        sys.exit()
    try :
        locdict = pickle.load(p)
    except :
        logger.exception('failed to open pickle')
        raise
    p.close()
    return locdict

class daemon_client(daemon.Daemon) :
    def __init__(self, pidfile, logger, stdin='/dev/null', 
                 stdout='/dev/null', stderr='/dev/null'):
        self.logger = logger
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def run(self) :
        main(self.logger)

if __name__ == '__main__' :
    pidfile = os.path.join(PIDDIR, 'beacon_client.pid')
    logger = build_logger()
    logger.debug('logger object built')
    daemon = daemon_client(pidfile, logger)
    #logger.debug('daemon created')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            logger.debug('starting')
            daemon.start()
        elif 'stop' == sys.argv[1]:
            logger.debug('stopping')
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            logger.debug('restarting')
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
