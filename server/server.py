#!/usr/bin/pypy

import logging
import logging.handlers
import os
import Queue
import re
import socket
import sys
import time
import threading

JOIN_STR = '^'
UDP_IP = '3.4.163.195'
UDP_PORT = 45454
results = {}
success_count = 0
fail_count = 0
DEBUG = False
HOMEDIR = '/home/jprice/programs/firegraph/server'
LOGFILE = os.path.join(HOMEDIR, 'beacon_server.log')

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
    file_format = logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(message)s")
    file_handler.setFormatter(file_format)
    log.addHandler(console_handler)
    log.addHandler(file_handler)
    return log

class ThreadGenImage(threading.Thread) :
    def __init__(self, aggregate_in_queue, logger) :
        threading.Thread.__init__(self, name='GenImage')
        self.aggregate_in_queue = aggregate_in_queue
        #self.outputfile = 'imagedata'
        self.outputfile = '/firegraph/firegraph-apache/realtime/imagedata'
        self.logger = logger

    def run(self) :
        counter = 0
        while True :
            aggregate = self.aggregate_in_queue.get()
            name = self.outputfile + '.' + '%02d' % counter + '.csv'
            f = open(name, 'w')
            f.write('''Latitude,Longitude,value\n''')
            for lat, longi in aggregate :
                f.write('''%3.1f,%3.1f,%d\n''' % (lat, longi, 
                                                  aggregate[(lat, longi)]))
            f.close()
            counter += 1
            if counter >= 100 :
                counter = 0
            #print 'aggregate saved'
            self.logger.info('aggregate saved to %s' % name)
            self.aggregate_in_queue.task_done()

class ThreadSuccessTrack(threading.Thread) :
    def __init__(self, lat_long_in_queue, aggregate_in_queue, logger) :
        threading.Thread.__init__(self, name='SuccessTrack')
        self.lat_long_in_queue = lat_long_in_queue
        self.aggregate_in_queue = aggregate_in_queue
        self.successful = 0
        self.aggregate = {}
        self.logger = logger

    def run(self) :
        while True:
            message = self.lat_long_in_queue.get()
            if message == 'push' :
                self.aggregate_in_queue.put(self.aggregate)
                self.aggregate = {}
                #print 'pushed aggregate'
                self.logger.info('pushed aggregate')
            else :
                lat, longi = message
                self.successful += 1
                try :
                    self.aggregate[(lat,longi)] += 1
                except KeyError :
                    self.aggregate[(lat,longi)] = 1
                if self.successful % 100 == 0 :
                    #print 'in SuccessTrack, # successes is %d, aggr size is %d, aggr queue size is %d' % (self.successful,
                    #        len(self.aggregate),
                    #        self.aggregate_in_queue.qsize())
                    self.logger.info("# successes is %d, aggregate size is %d" % (self.successful, len(self.aggregate)))
            self.lat_long_in_queue.task_done()

class ThreadLocationParse(threading.Thread) :
    """thread to process a location data."""
    def __init__(self, data_in_queue, lat_long_in_queue, logger) :
        threading.Thread.__init__(self, name='LocationParse')
        self.data_in_queue = data_in_queue
        self.lat_long_in_queue = lat_long_in_queue
        self.logger = logger

    def run(self) :
        counter = 0
        while True :
            data = self.data_in_queue.get()
            lat, longi = data.split(JOIN_STR)
            lat = float(lat)
            longi = float(longi)
            self.lat_long_in_queue.put((lat,longi))
            self.data_in_queue.task_done()
            counter += 1
            if counter % 200 == 0 :
                #print "in LocParse data_in: %d lat_in: %d" % (self.data_in_queue.qsize(), self.lat_long_in_queue.qsize())
                self.logger.info("data_in_q: %d lat_in_q: %d" % (self.data_in_queue.qsize(),
                            self.lat_long_in_queue.qsize()))

def server_loop(data_in_queue, lat_long_in_queue, logger) :
    global UDP_IP
    global UDP_PORT
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    sock.bind( (UDP_IP, UDP_PORT) )
    counter = 0
    try :
        while True :
            data, addr = sock.recvfrom( 1024 )
            ip, port = addr
            #print repr(data), repr(addr)
            try :
                results[ip] += 1
            except KeyError :
                results[ip] = 1
            counter += 1
            #print repr(data)
            #process_data(data, connection, cursor)
            data_in_queue.put(data)
            if counter >= 5000 :
                counter = 0
                lat_long_in_queue.put('push')
                logger.info('sending command to push out an aggregate')
    except KeyboardInterrupt :
        logger.info('received %d fail %d' % (success_count, fail_count))
        print 'received %d fail %d' % (success_count, fail_count)
        raise

def main() :
    logger = build_logger()
    data_in_queue = Queue.Queue()
    lat_long_in_queue = Queue.Queue()
    aggregate_in_queue = Queue.Queue()
    #processors = [ ThreadLocationParse(data_in_queue, lat_long_in_queue) for i in range(1) ]
    #for p in processors :
    #    p.setDaemon(True)
    #    p.start()
    processor = ThreadLocationParse(data_in_queue, lat_long_in_queue, logger)
    processor.setDaemon(True)
    processor.start()

    success = ThreadSuccessTrack(lat_long_in_queue, aggregate_in_queue, logger)
    success.setDaemon(True)
    success.start()

    genimage = ThreadGenImage(aggregate_in_queue, logger)
    genimage.setDaemon(True)
    genimage.start()
    try :
        server_loop(data_in_queue, lat_long_in_queue, logger)
    except KeyboardInterrupt :
        for k in sorted(results) :
            logger.info("%s\t%d" % (k, results[k]))
            print k, results[k]
        raise

if __name__ == '__main__' :
    main()
