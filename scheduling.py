import time 
import json 
import random

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

def randomScheduler(workerDetails):
    while 1:
        randomChoice = random.randrange(1, len(workerDetails))
        chosenWorker = workerDetails[randomChoice]
        if chosenWorker['slots'] > 0:
            print("chosen worker for random scheduler: ", chosenWorker)
            chosenWorker['slots'] -= 1      # capturing 1 slot 
            """
                execute task
            """
            chosenWorker['slots'] += 1      # freeing 1 slot 
            return 

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
            print("chosen worker for least loaded scheduler: ", i)
            i['slots'] -= 1     # capturing 1 slot
            """
                execute task
            """
            i['slots'] += 1     # freeing 1 slot 
            return 

def roundRobinScheduler(workerDetails):
    for i in range(len(workerDetails)):
        if workerDetails[i]['slots'] > 0:
            print("chosen worker by round robin scheduling: ", workerDetails[i])
            workerDetails[i]['slots'] -= 1   # capturing 1 slot
            """
                execute task
            """
            workerDetails[i]['slots'] += 1   # freeing 1 slot
            return 


file = open('Copy of config.json')
s = file.read()
data = json.loads(s)
workerDetails = data['workers']

# sorting workers by IDs
workerDetails = sortWorkersByIDs(workerDetails)
# print(workerDetails)

# testing the schedulers 
randomScheduler(workerDetails)
leastLoadedScheduler(workerDetails)
roundRobinScheduler(workerDetails)