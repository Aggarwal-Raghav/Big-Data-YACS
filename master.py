import socket
import sys
import json
import threading
import time
from scheduling import *
import random
import logging

logging.basicConfig(filename='logs.log',filemode = 'w', level=logging.INFO)

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
jobLengthReducer = []


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

        logging.info(str(time.time())+":Recieved Job from requests.py with ID:"+str(obj['job_id']))
        
        mapperList+=obj['map_tasks']
        reducerList.append(list(obj['reduce_tasks']))

        sem1.acquire()                          # sem1 - used only for execQueue variable
        execQueue+=obj['map_tasks']
        sem1.release()
        jobLength.append(len(obj['map_tasks']))         # Storing length of number of map tasks
        jobLengthReducer.append(len(obj['reduce_tasks']))
        connection.close()

def sendTaskRequest(workerJob, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", port))
        message=json.dumps(workerJob)
        s.send(message.encode())
        print(workerJob)
        logging.info(str(time.time())+":Sending Task request to Worker on port :"+str(port)+": with task_id :"+workerJob['task_id'])
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
        data = json.loads(data)
        print(data)
        logging.info(str(time.time())+":Completed task with ID :"+data)
        if data[2]=='R':
            jobIndex = int(data[0])                             #reducerIndex = JobId
            jobLengthReducer[jobIndex]-=1
            if jobLengthReducer[jobIndex]==0:
                logging.info(str(time.time())+":"+"Completed Job:"+data[0])


        print("Completed task. ID returned : ",data)                # eg: "0_M0 1"
        if data[2]=='M':
            reducerIndex = int(data[0])                             #reducerIndex = JobId
            jobLength[reducerIndex]-=1
            if jobLength[reducerIndex]==0:
                sem1.acquire()
                for job in reducerList[reducerIndex]:
                    execQueue.insert(0,job)
                sem1.release()

        sem.acquire()
        workerData[int(data[-1])-1]['slots']+=1
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
            if scheduleAlgo == 'RR':
                workerDetails = roundRobinScheduler(workerData, iterator ,lenOfWorker)
                sem.acquire()                                                               #WorkerDetails
                workerData[workerDetails['worker_id']-1]['slots']-=1
                sem.release()

            elif scheduleAlgo == 'RANDOM':
                workerDetails = randomScheduler(workerData)
                sem.acquire()
                workerData[workerDetails['worker_id']-1]['slots']-=1
                # print("chosenWorker for RANDOM: ", workerDetails)
                time.sleep(0.001)
                sem.release()

            elif scheduleAlgo == 'LL':
                workerDetails = leastLoadedScheduler(workerData)
                sem.acquire()
                # print("chosenWorker for LeastLoaded: ", workerDetails)
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
