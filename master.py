#importing the python libraries
import re
import socket
import sys
import json
import threading
import time

# Our implemention of scheduling algorithm
from scheduling import *

import random
import logging
import os

#For removing previous logs file
if os.path.exists("logs.log"):
    os.remove("logs.log")

#Function for creating log handler and intializing the log file for multiple logging
def createLogHandler(log_file):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file)     #creating the handle for the log file
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')         #Format for our log file
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger       # Returning the file handler (in_built function)


log_file = 'logs.log'       #Name of our log file
logger = createLogHandler(log_file)

#Initializing of semaphores 
sem = threading.Semaphore()
sem1 = threading.Semaphore()

iterator = 0

#Taking the command line argument
pathConf = sys.argv[1]
scheduleAlgo = sys.argv[2]

conf = open(pathConf,'r')       #Reading the config file
conf = conf.read()
confData = json.loads(conf)

#list of jobs
workerData = confData['workers']
lenOfWorker = len(workerData)       #Length of the number of workers


#Initializing empty lists for processing
execQueue = []
mapperList = []
reducerList = []
jobLength = []
jobLengthReducer = []


#Function to receive input from  request.py file
def recRequest():
    global mapperList
    global reducerList
    global execQueue
    
    #Creating the socket
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #setting the port at 5000
    serverAddress = ("localhost", 5000)             #Port for receiving requests from Requests.py
    sock1.bind(serverAddress)
    sock1.listen(1)

    while 1:
        connection, address = sock1.accept()
        data = connection.recv(2048)
        obj = json.loads(data.decode("utf-8"))      # Data -> string    json.loads - data -> dictionary

        logger.info(str(time.time())+": Recieved Job from requests.py with ID :"+str(obj['job_id']))
        
        mapperList+=obj['map_tasks']
        reducerList.append(list(obj['reduce_tasks']))
    
        #sem1 -> used only for execQueue variable
        sem1.acquire()                              
        execQueue+=obj['map_tasks']
        sem1.release()

        jobLength.append(len(obj['map_tasks']))             #Storing length of number of map tasks
        jobLengthReducer.append(len(obj['reduce_tasks']))   #Storing length of number of reduce tasks
        connection.close()

#Function to send task data to specified worker on port 
def sendTaskRequest(workerJob, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", port))
        message=json.dumps(workerJob)
        s.send(message.encode())
        #print(workerJob)
        s.close()

#Function to listen for task completion updates from worker
def workerListen():
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #Setting the port to 5001
    serverAddress = ("localhost", 5001)
    sock2.bind(serverAddress)
    sock2.listen(1)

    while 1:
        connection, address = sock2.accept()
        data = connection.recv(1024).decode("utf-8")
        data = json.loads(data)
        pattern = "(\d*)_(.).*"
        matched = re.search(pattern, data)
        print(matched.group(0))
        #print(data)
        #Checking if all reducers of specific jobs are complete
        if matched.group(2)=='R':
            #jobIndex = int(data[0]) 
            jobIndex = int(matched.group(1)) 
            jobLengthReducer[jobIndex]-=1
            if jobLengthReducer[jobIndex]==0:
                logger.info(str(time.time())+":"+" Completed Job :"+str(jobIndex))


        print("Completed task. ID returned : ",data)     #eg: "0_M0 1"
        #Checking if all mappers of specific jobs are complete
        if matched.group(2)=='M':
            reducerIndex = int(matched.group(1))                             #reducerIndex = JobId
            #reducerIndex = int(data[0])                             #reducerIndex = JobId
            jobLength[reducerIndex]-=1
            if jobLength[reducerIndex]==0:
                #Acquiring semaphore and including reducer tasks in execution queue
                sem1.acquire()
                for job in reducerList[reducerIndex]:
                    execQueue.insert(0,job)
                sem1.release()

        #Increasing slots after task completion
        sem.acquire()
        workerData[int(data[-1])-1]['slots']+=1
        sem.release()
        connection.close()

#Function for selecting workers based on scheduling algorithm
def workerScheduling():
    global iterator
    global execQueue
    while 1:
        if execQueue:      
            #print("execution Queue : ",execQueue)
            sem1.acquire()    
            v = execQueue.pop(0)
            sem1.release()

            #Round Robin algorithm
            if scheduleAlgo == 'RR':
                workerDetails = roundRobinScheduler(workerData, iterator ,lenOfWorker)
                iterator+=1

            #Random scheduling 
            elif scheduleAlgo == 'RANDOM':
                workerDetails = randomScheduler(workerData)

            #Least Loaded algorithm 
            elif scheduleAlgo == 'LL':
                workerDetails = leastLoadedScheduler(workerData)

            sem.acquire()
            workerData[workerDetails['worker_id']-1]['slots']-=1
            sem.release()
            #logging.info(workerData)
            #logging.info(execQueue)
            sendTaskRequest(v, workerDetails['port'])

#Creating thread for receiveing job request from requests.py
thread1 = threading.Thread(target = recRequest)
thread1.start()

#Creating thread scheduling workers
thread2 = threading.Thread(target = workerScheduling)
thread2.start()

#Creating thread for listening updates from workers
thread3 = threading.Thread(target = workerListen)
thread3.start()
