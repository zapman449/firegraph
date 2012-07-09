#!/usr/bin/python

import socket

UDP_IP = '3.4.163.195'
UDP_PORT = 45454

sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
sock.bind( (UDP_IP, UDP_PORT) )

results = {}

try :
    while True :
        data, addr = sock.recvfrom( 1024 )
        ip, port = addr
        #print repr(data), repr(addr)
        if ip not in results :
            results[ip] = 0
        results[ip] += 1
except KeyboardInterrupt :
    for k in sorted(results) :
        print k, results[k]
    raise
