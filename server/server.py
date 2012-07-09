#!/usr/bin/python

import csv
import os
import os.path
import re
import socket
import sqlite3
import sys

UDP_IP = '3.4.163.195'
UDP_PORT = 45454
results = {}
SQLDB = 'zips.sqlite'
CSV = 'zipcode.csv'

def server_loop(connection, cursor) :
    global UDP_IP
    global UDP_PORT
    re_zip = re.compile('\d\d\d\d\d')
    re_loc = re.compile('US[A-Z][A-Z]\d\d\d\d')
    re_loc_oob = re.compile('[A-Z][A-Z][A-Z][A-Z]\d\d\d\d')
    re_state = re.compile(' [A-Z][A-Z] ')
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    sock.bind( (UDP_IP, UDP_PORT) )
    success_count = 0
    fail_count = 0
    try :
        while True :
            data, addr = sock.recvfrom( 1024 )
            ip, port = addr
            #print repr(data), repr(addr)
            if ip not in results :
                results[ip] = 0
            results[ip] += 1
            #print repr(data)
            string = data.replace('+', ' ').replace('+', ' ')
            found = False
            zsearch = re_zip.search(string)
            if zsearch == None :
                pass
            else :
                zipcode = zsearch.group()
                lat, longi = zip_to_ll(zipcode, connection, cursor)
                found = True
            if not found :
                lsearch = re_loc.search(string)
                if lsearch == None :
                    pass
                else :
                    loccode = lsearch.group()
                    lat, longi = loc_to_ll(loccode, connection, cursor)
                    found = True
            if found :
                success_count += 1
            else :
                fail_count += 1
                print repr(string)
    except KeyboardInterrupt :
        print 'success %d fail %d' % (success_count, fail_count)
        raise

def zip_to_ll(zipcode, connection, cursor) :
    cursor.execute('SELECT Latitude, Longitude FROM ZipCodes WHERE ZipCode = ?',
            (zipcode,))
    row = cursor.fetchone()
    if row == None :
        return None, None
    else :
        return row

def loc_to_ll(loccode, connection, cursor) :
    cursor.execute('SELECT City FROM LocCodes WHERE LocCode = ?', (loccode,))
    row = cursor.fetchone()
    if row == None :
        return None, None
    else :
        city = row[0]
    state = loccode[2:4]
    cursor.execute('SELECT Latitude, Longitude FROM ZipCodes WHERE City = ? AND State = ?', (city,state))
    row = cursor.fetchone()
    if row == None :
        return None, None
    else :
        return row

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
    connection, cursor = init_db()
    try :
        server_loop(connection, cursor)
    except KeyboardInterrupt :
        for k in sorted(results) :
            print k, results[k]
        cursor.close()
        connection.close()
        raise

if __name__ == '__main__' :
    main()
