#!/usr/bin/python

import csv
import os
import os.path
import sqlite3
import sys

SQLDB = 'zips.sqlite'
ZIPCODES = 'zipcode.csv'
LOCCODES = 'loccodes.csv'

def init_db() :
    global SQLDB
    try :
        connection = sqlite3.connect(SQLDB)
        cursor = connection.cursor()
    except  sqlite3.Error, e:
        print "Error %s:" % e.args[0]
        sys.exit()
    try :
        cursor.execute("SELECT Version FROM DbVersion")
    except sqlite3.Error, e:
        if e.args[0].startswith("no such table: ") :
            # proxy test for an empty DB
            make_tables(connection, cursor)
            load_zips(connection, cursor)
            load_locs(connection, cursor)
            #index_db(connection, cursor)
    return connection, cursor

def make_tables(connection, cursor) :
    """makes blank tables"""
    #print 'create DbVersion'
    cursor.execute("CREATE TABLE DbVersion(Version INT)")
    cursor.execute("INSERT INTO DbVersion(Version) VALUES (1);")
    #print 'create Fabrics'
    cursor.execute("CREATE TABLE ZipCodes( ZipCode CHAR(5) UNIQUE NOT NULL ON CONFLICT ROLLBACK, City CHAR(20), State CHAR(5), Latitude CHAR(10), Longitude CHAR(10))")
    cursor.execute("CREATE TABLE LocCodes( LocCode CHAR(8) UNIQUE NOT NULL ON CONFLICT ROLLBACK, City CHAR(20) )")
    connection.commit()
    cursor.execute("CREATE INDEX ZipCodeIndex on ZipCodes (ZipCode)")
    connection.commit()

def load_zips(connection, cursor) :
    global ZIPCODES
    f = open(ZIPCODES, 'r')
    counter = 0
    try :
        reader = csv.reader(f)
        for row in reader :
            load_row(row, connection, cursor)
            counter += 1
        if counter % 1000 == 0 :
            connection.commit()
    finally:
        f.close()
        connection.commit()

def load_row(row, connection, cursor) :
    if len(row) == 0 :
        return True
    zipcode, city, state, lat, longi, tz, dst = row
    cursor.execute("INSERT INTO ZipCodes VALUES (?, ?, ?, ?, ?);",
                              (zipcode, city, state, lat, longi))
    return True

def load_locs(connection, cursor) :
    global LOCCODES
    f = open(LOCCODES, 'r')
    counter = 0
    try :
        reader = csv.reader(f)
        for row in reader :
            load_loc(row, connection, cursor)
            counter += 1
        if counter % 1000 == 0 :
            connection.commit()
    finally:
        f.close()
        connection.commit()

def load_loc(row, connection, cursor) :
    if len(row) == 0 :
        return True
    loccode, city = row
    cursor.execute("INSERT INTO LocCodes VALUES (?, ?);", (loccode, city))
    return True
#def index_db(connection, cursor) :
#    cursor.execute("CREATE INDEX IF NO EXISTS ")

def main() :
    global SQLDB
    connection, cursor = init_db()
    connection.commit()

if __name__ == '__main__' :
    main()
