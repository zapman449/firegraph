#!/usr/bin/python

import csv
import os
import os.path
import socket
import sqlite3
import sys

UDP_IP = '3.4.163.195'
UDP_PORT = 45454
results = {}
SQLDB = 'zips.sqlite'
CSV = 'zipcode.csv'

def server_loop() :
    global UDP_IP
    global UDP_PORT
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    sock.bind( (UDP_IP, UDP_PORT) )
    while True :
        data, addr = sock.recvfrom( 1024 )
        ip, port = addr
        #print repr(data), repr(addr)
        if ip not in results :
            results[ip] = 0
        results[ip] += 1
        print repr(data)

def init_db() :
    global SQLDB
    try :
        connection = sqlite3.connect(SQLDB)
        cursor = connection.cursor()
    except  sqlite3.Error, e:
        print "Error %s:" % e.args[0]
        print 'hey... run load_db'
        sys.exit()
    try :
        cursor.execute("SELECT Version FROM DbVersion")
    except sqlite3.Error, e:
        if e.args[0].startswith("no such table: ") :
            # proxy test for an empty DB
            print 'hey... run load_db'
            sys.exit()
    return connection, cursor

def main() :
    global SQLDB
    connection, cursor = init_db()
    try :
        server_loop()
    except KeyboardInterrupt :
        for k in sorted(results) :
            print k, results[k]
        cursor.close()
        connection.close()
        raise

if __name__ == '__main__' :
    main()
