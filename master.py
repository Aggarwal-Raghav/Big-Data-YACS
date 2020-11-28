import socket
import sys
import json
import threading
import time
from scheduling import *
import random

sem = threading.Semaphore()
sem1 = threading.Semaphore()
LIST=[]

iterator = 0

pathConf = sys.argv[1]
conf = open(pathConf,'r')
conf = conf.read()
confData = json.loads(conf)
#list of jobs
workerData = confData['workers']
print(workerData)
lenOfWorker = len(workerData)

execQueue = []
mapperList = []
reducerList = []
jobLength = []


def recRequest():
    global mapperList
    global reducerList
    global execQueue

    sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", 5000)
    sock1.bind(serverAddress)
    sock1.listen(1)

    while 1:
        connection, address = sock1.accept()
        data = connection.recv(2048)
        if data==b'':
            sem1.release()
            break
        obj = json.loads(data.decode("utf-8"))
        
        mapperList+=obj['map_tasks']
        reducerList+=list(obj['reduce_tasks'])

        sem1.acquire()
        execQueue+=obj['map_tasks']
        sem1.release()
        jobLength.append(len(obj['map_tasks']))

    connection.close()

def sendTaskRequest(workerJob, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", port))
        message=json.dumps(workerJob)
        s.send(message.encode())

def workerListen():
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", 5001)
    sock2.bind(serverAddress)
    sock2.listen(1)

    while 1:
        connection, address = sock2.accept()
        data = connection.recv(1024).decode("utf-8")
        print(data)
        if data[3]=='M':
            jobLength[int(data[1])]-=1
            if jobLength[int(data[1])]==0:
                sem1.acquire()
                execQueue.insert(0,reducerList.pop(0))
                sem1.release()

        sem.acquire()
        #we have to add worker id in this
        print(data[1],data[-2])
        workerData[int(data[-2])-1]['slots']+=1
        sem.release()
        
    connection.close()

def workerScheduling():
    global iterator
    global execQueue
    while 1:
        if execQueue:
            workerDetails = roundRobinScheduler(workerData, iterator ,lenOfWorker)
            
            sem1.acquire()
            sendTaskRequest(execQueue.pop(0), workerDetails['port'])
            sem1.release() 

            sem.acquire()
            workerData[iterator%lenOfWorker]['slots']-=1
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
