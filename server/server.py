#!/usr/bin/python

#import csv
#import os
#import os.path
import Queue
import re
import socket
import sqlite3
import sys
import threading

UDP_IP = '3.4.163.195'
UDP_PORT = 45454
results = {}
#SQLDB = 'zips.sqlite'
CSV = 'zipcode.csv'
success_count = 0
fail_count = 0

class ThreadSuccessTrack(threading.Thread) :
    def __init__(self, lat_long_in_queue) :
        threading.Thread.__init__(self)
        self.lat_long_in_queue = lat_long_in_queue
        self.successful = 0

    def run(self) :
        while True:
            lat, longi = self.lat_long_in_queue.get()
            self.successful += 1
            if self.successful % 100 == 0 :
                print 'in SuccessTrack, # successes is %d' % self.successful
            self.lat_long_in_queue.task_done()

class ThreadFailTrack(threading.Thread) :
    def __init__(self, fail_in_queue) :
        threading.Thread.__init__(self)
        self.fail_in_queue = fail_in_queue
        self.failures = 0

    def run(self) :
        while True:
            fail = self.fail_in_queue.get()
            self.failures += 1
            if self.failures % 100 == 0 :
                print 'in FailTrack, # failures is %d' % self.failures
            self.fail_in_queue.task_done()

class ThreadLocationParse(threading.Thread) :
    """thread to process a location data."""
    def __init__(self, data_in_queue, lat_long_in_queue, fail_in_queue) :
        threading.Thread.__init__(self)
        self.data_in_queue = data_in_queue
        self.lat_long_in_queue = lat_long_in_queue
        self.fail_in_queue = fail_in_queue
        self.connection = sqlite3.connect('zips.sqlite', 
                                          check_same_thread = False)
        self.cursor = self.connection.cursor()
        self.re_zip = re.compile('\d\d\d\d\d')
        self.re_loc = re.compile('US[A-Z][A-Z]\d\d\d\d')
        #self.re_loc_oob = re.compile('[A-Z][A-Z][A-Z][A-Z]\d\d\d\d')
        #self.re_state = re.compile(' [A-Z][A-Z] ')

    def run(self) :
        counter = 0
        while True :
            data = self.data_in_queue.get()
            string = data.replace('+', ' ').replace('+', ' ')
            found = False
            zsearch = self.re_zip.search(string)
            if zsearch == None :
                pass
            else :
                zipcode = zsearch.group()
                lat, longi = self.zip_to_ll(zipcode)
                found = True
            if not found :
                lsearch = self.re_loc.search(string)
                if lsearch == None :
                    pass
                else :
                    loccode = lsearch.group()
                    lat, longi = self.loc_to_ll(loccode)
                    found = True
            if found :
                self.lat_long_in_queue.put((lat,longi))
            else :
                self.fail_in_queue.put(string)
                #print repr(string)
            self.data_in_queue.task_done()
            counter += 1
            if counter % 200 == 0 :
                print "in LocParse data_in: %d lat_in: %d fail_in: %d" % (self.data_in_queue.qsize(), self.lat_long_in_queue.qsize(), self.fail_in_queue.qsize())

    def zip_to_ll(self, zipcode) :
        self.cursor.execute("""SELECT Latitude, Longitude 
                               FROM ZipCodes WHERE ZipCode = ?""", (zipcode,))
        row = self.cursor.fetchone()
        if row == None :
            return None, None
        else :
            return row

    def loc_to_ll(self, loccode) :
        self.cursor.execute("""SELECT City 
                               FROM LocCodes WHERE LocCode = ?""", (loccode,))
        row = self.cursor.fetchone()
        if row == None :
            return None, None
        else :
            city = row[0]
        state = loccode[2:4]
        self.cursor.execute("""SELECT Latitude, Longitude 
                               FROM ZipCodes WHERE City = ? AND State = ?""",
                               (city,state))
        row = self.cursor.fetchone()
        if row == None :
            return None, None
        else :
            return row

def server_loop(data_in_queue) :
    global UDP_IP
    global UDP_PORT
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    sock.bind( (UDP_IP, UDP_PORT) )
    try :
        while True :
            data, addr = sock.recvfrom( 1024 )
            ip, port = addr
            #print repr(data), repr(addr)
            if ip not in results :
                results[ip] = 0
            results[ip] += 1
            #print repr(data)
            #process_data(data, connection, cursor)
            data_in_queue.put(data)
    except KeyboardInterrupt :
        print 'success %d fail %d' % (success_count, fail_count)
        raise

#def init_db() :
#    global SQLDB
#    try :
#        connection = sqlite3.connect(SQLDB)
#        cursor = connection.cursor()
#    except  sqlite3.Error, e:
#        print "Error %s:" % e.args[0]
#        print 'hey... run load_db'
#        sys.exit()
#    try :
#        cursor.execute("SELECT Version FROM DbVersion")
#    except sqlite3.Error, e:
#        if e.args[0].startswith("no such table: ") :
#            # proxy test for an empty DB
#            print 'hey... run load_db'
#            sys.exit()
#    return connection, cursor

def main() :
    #connection, cursor = init_db()
    data_in_queue = Queue.Queue()
    lat_long_in_queue = Queue.Queue()
    fail_in_queue = Queue.Queue()
    processors = [ ThreadLocationParse(data_in_queue, lat_long_in_queue, fail_in_queue) for i in range(25) ]
    for p in processors :
        p.setDaemon(True)
        p.start()
    success = ThreadSuccessTrack(lat_long_in_queue)
    success.setDaemon(True)
    success.start()
    fail = ThreadFailTrack(fail_in_queue)
    fail.setDaemon(True)
    fail.start()
    try :
        server_loop(data_in_queue)
    except KeyboardInterrupt :
        for k in sorted(results) :
            print k, results[k]
        raise

if __name__ == '__main__' :
    main()
