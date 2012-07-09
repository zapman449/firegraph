#!/usr/bin/python

import socket

UDP_IP = '3.4.163.195'
UDP_PORT = 45454

sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
sock.bind( (UDP_IP, UDP_PORT) )

data = {}

while True :
    data, addr = sock.recvfrom( 1024 )
    data.setdefault(addr, 0) += 1
