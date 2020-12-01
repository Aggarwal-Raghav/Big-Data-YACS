#Importing the python libraries
import time 
import json 
import random


#Function to sort the list of worker on basis of worker id
def sortWorkersByIDs(workerDetails):
    listNew = []
    for i in workerDetails:
        listNew.append(i['worker_id'])
    listNew = sorted(listNew)
    sortedList = []
    for i in listNew:
        for j in workerDetails:
            if(i == j['worker_id']):
                sortedList.append(j)
    return sortedList

#Fuction for Random Scheduling
def randomScheduler(workerDetails):
    while 1:
        randomChoice = random.randrange(0, len(workerDetails))
        chosenWorker = workerDetails[randomChoice]
        if chosenWorker['slots'] > 0:
            return chosenWorker

#Fuction for Least loaded schduling
def leastLoadedScheduler(workerDetails):
    maxWorkerID = -1
    maxWorkerSlots = -1
    while 1:
        for i in workerDetails:
            if i['slots'] > maxWorkerSlots:
                maxWorkerSlots = i['slots']
                maxWorkerID = i['worker_id']
        if maxWorkerSlots == -1 or maxWorkerSlots == 0:
            print("Waiting(1 sec) to find free slots...")       # if not slots free then wait 1 sec
            time.sleep(1)
        else:
            break       # worker with max slot found

    for i in workerDetails:
        if i['worker_id'] == maxWorkerID:
            return i

#Function for Round Robin scheduling
def roundRobinScheduler(workerDetails, i, Len):
    i=i%Len
    while workerDetails[i]['slots']<=0:
        i = (i+1)%Len
    return workerDetails[i]


