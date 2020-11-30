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
scheduleAlgo = sys.argv[2]
conf = open(pathConf,'r')
conf = conf.read()
confData = json.loads(conf)
#list of jobs
workerData = confData['workers']
lenOfWorker = len(workerData)

print("SCHEDULING ALGO: ", scheduleAlgo)
execQueue = []
mapperList = []
reducerList = []
jobLength = []


def recRequest():
    global mapperList
    global reducerList
    global execQueue
    
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serverAddress = ("localhost", 5000)                             #Port for receiving requests from Requests.py
    sock1.bind(serverAddress)
    sock1.listen(1)

    while 1:
        connection, address = sock1.accept()
        data = connection.recv(2048)
        obj = json.loads(data.decode("utf-8"))                      # Data -> string,       json.loads - data -> dictionary
        
        mapperList+=obj['map_tasks']
        reducerList.append(list(obj['reduce_tasks']))

        sem1.acquire()                          # sem1 - used only for execQueue variable
        execQueue+=obj['map_tasks']
        sem1.release()
        jobLength.append(len(obj['map_tasks']))         # Storing length of number of map tasks
        connection.close()

def sendTaskRequest(workerJob, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", port))
        message=json.dumps(workerJob)
        s.send(message.encode())
        s.close()

def workerListen():
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serverAddress = ("localhost", 5001)
    sock2.bind(serverAddress)
    sock2.listen(1)

    while 1:
        connection, address = sock2.accept()
        data = connection.recv(1024).decode("utf-8")
        print("Completed task. ID returned : ",data)                # eg: "0_M0 1"
        if data[3]=='M':
            reducerIndex = int(data[1])                             #reducerIndex = JobId
            jobLength[reducerIndex]-=1
            if jobLength[reducerIndex]==0:
                sem1.acquire()
                for job in reducerList[reducerIndex]:
                    execQueue.insert(0,job)
                sem1.release()

        sem.acquire()
        workerData[int(data[-2])-1]['slots']+=1
        sem.release()
        connection.close()

def workerScheduling():
    global iterator
    global execQueue
    while 1:
        if execQueue:      
            #print("execution Queue : ",execQueue)
            sem1.acquire()    
            v = execQueue.pop(0)
            sem1.release()
            """
            print('*'*10)
            print("Sending Task Request")
            print(v)
            print("On worker",workerDetails)
            print("*"*10)
            """
            if scheduleAlgo == 'RR':
                sem.acquire()                                                               #WorkerDetails
                workerDetails = roundRobinScheduler(workerData, iterator ,lenOfWorker)
                workerData[workerDetails['worker_id']-1]['slots']-=1
                sem.release()

            elif scheduleAlgo == 'RANDOM':
                sem.acquire()
                workerDetails = randomScheduler(workerData)
                workerData[workerDetails['worker_id']-1]['slots']-=1
                sem.release()
                # print("chosen worker ", workerDetails['port'])
            elif scheduleAlgo == 'LL':
                sem.acquire()
                workerDetails = leastLoadedScheduler(workerData)
                # print("MAX SLOTS: ", workerData[workerDetails['worker_id']-1]['slots'])
                workerData[workerDetails['worker_id']-1]['slots']-=1
                sem.release()

            sendTaskRequest(v, workerDetails['port'])
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
