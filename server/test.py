#!/usr/bin/python

import sqlite3
import timeit

def test1() :
    global cur
    for z in l :
        cur.execute('select Latitude, Longitude from ZipCodes where ZipCode = ?', (z,))

def main() :
    global cur
    q = open('randzips10k', 'r')
    for line in q:
        l.append(line.strip())
    t = timeit.Timer('test1()', "from __main__ import test1")
    #elapsed = (10 * t.timeit(number=100000))
    #print elapsed
    print 'starting'
    print t.timeit(number=10)

if __name__ == '__main__' :
    conn = sqlite3.connect('zips.sqlite')
    cur = conn.cursor()
    l = []
    main()
