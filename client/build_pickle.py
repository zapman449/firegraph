#!/usr/bin/python

import cPickle as pickle


import csv
import os
import os.path
import sys

picklefile = 'loc.pickle'
ZIPCODES = 'zipcode.csv'
LOCCODES = 'loccodes.csv'

locdict = {}
citydict = {}

def load_zips() :
    global ZIPCODES
    f = open(ZIPCODES, 'r')
    counter = 0
    try :
        reader = csv.reader(f)
        for row in reader :
            load_row(row)
            counter += 1
    finally:
        f.close()

def load_row(row) :
    if len(row) == 0 :
        return True
    zipcode, city, state, lat, longi, tz, dst = row
    locdict[zipcode] = (lat, longi)
    citydict[(city, state)] = (lat, longi)
    return True

def load_locs() :
    global LOCCODES
    f = open(LOCCODES, 'r')
    counter = 0
    try :
        reader = csv.reader(f)
        for row in reader :
            load_loc(row)
            counter += 1
    finally:
        f.close()

def load_loc(row) :
    if len(row) == 0 :
        return True
    loccode, city = row
    state = loccode[2:4]
    try :
        lat, longi = citydict[(city, state)]
    except KeyError :
        #print 'failed to find -- %s -- %s --' % (city, state)
        return True
    locdict[loccode] = (lat, longi)
    return True

def main() :
    global picklefile
    load_zips()
    load_locs()
    #print len(locdict), len(citydict)
    pickleout = open(picklefile, 'wb')
    pickle.dump(locdict, pickleout)
    pickleout.close()

if __name__ == '__main__' :
    main()
