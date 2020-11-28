import socket
import sys
import json
import threading
import time
from scheduling import *
import random

sem = threading.Semaphore()
LIST=[]
iterator = 0

pathConf = sys.argv[1]
conf = open(pathConf,'r')
conf = conf.read()
confData = json.loads(conf)
#list of jobs
workerData = confData['workers']
lenOfWorker = len(workerData)

def recRequest():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", 5000)
    sock.bind(serverAddress)
    sock.listen(1)

    while 1:
        connection, address = sock.accept()
        sem.acquire()
        data = connection.recv(2048)
        if data==b'':
            sem.release()
            break
        obj = json.loads(data.decode("utf-8"))
        LIST.append(obj)
        sem.release()
        print(obj)
    connection.close()

def sendJobRequest(workerJob, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", port))
        message=json.dumps(workerJob)
        s.send(message.encode())

def workerListen():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", 5001)
    sock.bind(serverAddress)
    sock.listen(1)

    while 1:
        connection, address = sock.accept()
        data = int(connection.recv(1024).decode("utf-8"))

        sem.acquire()
        workerData[data-1]['slots']+=1
        sem.release()
        
    connection.close()

def workerScheduling():
    while LIST:
        workerDetails = roundRobinScheduler(workerData, iterator ,lenOfWorker)
        sendJobRequest(LIST.pop(0), workerDetails['port'])

        sem.acquire()
        workerData[iterator]['slots']-=1
        sem.release() 
        iterator+=1
        

thread1 = threading.Thread(target = recRequest)
thread1.start()

thread2 = threading.Thread(target = workerScheduling)
thread2.start()

thread3 = threading.Thread(target = workerListen)
thread3.start()

thread1.join()
thread2.join()
thread3.join()

