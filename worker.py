import sys
import socket
import numpy
import json

port = sys.argv[1]
Id = sys.argv[2]

def workerListen():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", port)
    sock.bind(serverAddress)
    sock.listen(1)

    while 1:
        connection, address = sock.accept()
        data = connection.recv(1024)
        jobData = json.loads(data)
    connection.close()

