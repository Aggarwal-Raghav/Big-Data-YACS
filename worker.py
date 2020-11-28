import sys
import socket
import numpy
import json

port = sys.argv[1]
id = sys.argv[2]

class worker:
    def __init__(self, id, port):
        self.id = id
        self.port = port

worker1 = worker() 
worker2 = worker() 
worker3 = worker() 

