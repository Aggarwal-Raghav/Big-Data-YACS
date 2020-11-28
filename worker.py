import sys
import socket
import numpy
import json
import threading
import time
import random

port = int(sys.argv[1])
Id = sys.argv[2]

def taskRun(taskData):
    print('hello',taskData)
    dur = int(taskData['duration'])
    time.sleep(dur)
    dataToSend =  taskData['task_id'] + " " +Id
    print(dataToSend)
    sendToMaster(dataToSend)

def workerListen():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", port)
    sock.bind(serverAddress)
    sock.listen(1)

    while 1:
        connection, address = sock.accept()
        data = connection.recv(1024).decode("utf-8")
        taskData = json.loads(data)
        print("inside workerListen")
        print(data)
        print(taskData)
        #problem is here or there

        threadCreate = threading.Thread(target = taskRun, args=(taskData,))
        threadCreate.start()
        threadCreate.join()
    connection.close()

def sendToMaster(ID):
    #has to send task id + worker id
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 5001))
        message=json.dumps(ID)
        s.send(message.encode())

thread1 = threading.Thread(target = workerListen)
thread1.start()
thread1.join()
