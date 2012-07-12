#!/usr/bin/pypy

#import csv
#import os
#import os.path
import Queue
import random
import re
import socket
import sqlite3
import sys
import time
import threading

JOIN_STR = '^'
UDP_IP = '3.4.163.195'
UDP_PORT = 45454
results = {}
#SQLDB = 'zips.sqlite'
CSV = 'zipcode.csv'
success_count = 0
fail_count = 0

class ThreadGenImage(threading.Thread) :
    def __init__(self, aggregate_in_queue) :
        threading.Thread.__init__(self, name='GenImage')
        self.aggregate_in_queue = aggregate_in_queue
        self.outputfile = 'imagedata'

    def run(self) :
        counter = 1
        while True :
            aggregate = self.aggregate_in_queue.get()
            name = self.outputfile + '.' + '%02d' % counter
            f = open(name, 'w')
            for lat, longi in aggregate :
                f.write('''"%3.1f","%3.1f","%d"\n''' % (lat, longi, 
                                                  aggregate[(lat, longi)]))
            f.close()
            counter += 1
            print 'aggregate saved'
            self.aggregate_in_queue.task_done()

class ThreadSuccessTrack(threading.Thread) :
    def __init__(self, lat_long_in_queue, aggregate_in_queue, rand) :
        threading.Thread.__init__(self, name='SuccessTrack')
        self.lat_long_in_queue = lat_long_in_queue
        self.aggregate_in_queue = aggregate_in_queue
        self.successful = 0
        #self.fname = 'output.%03d' % rand
        #self.f = open(self.fname, 'w')
        self.aggregate = {}

    def run(self) :
        while True:
            message = self.lat_long_in_queue.get()
            if message == 'push' :
                self.aggregate_in_queue.put(self.aggregate)
                self.aggregate = {}
                print 'pushed aggregate'
            else :
                lat, longi = message
                self.successful += 1
                #self.f.write("%s %s\n" % (lat, longi))
                try :
                    self.aggregate[(lat,longi)] += 1
                except KeyError :
                    self.aggregate[(lat,longi)] = 1
                if self.successful % 100 == 0 :
                    print 'in SuccessTrack, # successes is %d, aggr size is %d, aggr queue size is %d' % (self.successful,
                            len(self.aggregate),
                            self.aggregate_in_queue.qsize())
            self.lat_long_in_queue.task_done()

class ThreadLocationParse(threading.Thread) :
    """thread to process a location data."""
    def __init__(self, data_in_queue, lat_long_in_queue) :
        threading.Thread.__init__(self, name='LocationParse')
        self.data_in_queue = data_in_queue
        self.lat_long_in_queue = lat_long_in_queue

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
                print "in LocParse data_in: %d lat_in: %d" % (self.data_in_queue.qsize(), self.lat_long_in_queue.qsize())

#class ThreadTimedAggrPush(threading.Thread) :
#    #def __init__(self, aggregate_in_queue, seconds_delay=10) :
#    def __init__(self, aggregate_in_queue) :
#        threading.Thread.__init__(self, name='timedaggrpush')
#        self.aggregate_in_queue = aggregate_in_queue
#        #self.seconds_delay = seconds_delay
#    def run() :
#        while True :
#            #time.sleep(self.seconds_delay)
#            time.sleep(10)
#            self.aggregate_in_queue.put('push')

def server_loop(data_in_queue, lat_long_in_queue) :
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
    except KeyboardInterrupt :
        print 'received %d fail %d' % (success_count, fail_count)
        raise

def main() :
    data_in_queue = Queue.Queue()
    lat_long_in_queue = Queue.Queue()
    aggregate_in_queue = Queue.Queue()
    processors = [ ThreadLocationParse(data_in_queue, lat_long_in_queue) for i in range(1) ]
    for p in processors :
        p.setDaemon(True)
        p.start()
    success = ThreadSuccessTrack(lat_long_in_queue, aggregate_in_queue, random.randint(0,1000))
    success.setDaemon(True)
    success.start()
    #push_timer = ThreadTimedAggrPush(aggregate_in_queue, seconds_delay=10)
    #push_timer = ThreadTimedAggrPush(aggregate_in_queue)
    #push_timer.setDaemon(True)
    #push_timer.start()
    genimage = ThreadGenImage(aggregate_in_queue)
    genimage.setDaemon(True)
    genimage.start()
    try :
        server_loop(data_in_queue, lat_long_in_queue)
    except KeyboardInterrupt :
        for k in sorted(results) :
            print k, results[k]
        raise

if __name__ == '__main__' :
    main()
