import socket
import sys
import json

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#s = socket.socket()
port = 5000
s.connect(("localhost", port))
while 1:
    object = json.loads(s.recv(2048))
    print(object)
s.close() 

