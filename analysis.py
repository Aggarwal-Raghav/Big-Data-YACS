#Importing the python libraries
import re
import numpy as np
import matplotlib.pyplot as plt

#Pattern for detecting job start and end
patJobStart = '^INFO:root:([0-9.]*):.*:(\d*)$'

#Pattern for detecting task start
patTaskStart = '^INFO:root:([0-9.]*):.*:(\d*):.*:(.*)$'

#Pattern for detecting task end
patTaskEnd = '^INFO:root:([0-9.]*):.*:(\S*) (\d)$'

#Reading the log file
logs = open('logs.log','r')
log_data = logs.readline()

#Initializing data structures
jobs = dict()
tasks = dict()
worker1 = []
count1 = 0
worker2 = []
count2 = 0
worker3 = []
count3 = 0

#Processing the log file line by line
while log_data:
    job_match = re.search(patJobStart,log_data)
    if job_match:
        t = float(job_match.group(1))
        jID = int(job_match.group(2))
        if jID not in jobs:
            jobs[jID] = t
        else:
            jobs[jID] = t - jobs[jID]
    task_match_start = re.search(patTaskStart,log_data)
    if task_match_start:
        #print(task_match_start.group(1),task_match_start.group(2),task_match_start.group(3))
        t = float(task_match_start.group(1))
        port = int(task_match_start.group(2))
        tID = task_match_start.group(3)
        
        if tID not in tasks:
            tasks[tID] = t
        
        #To detect count of worker 1
        if port%3999 == 1:
            count1 += 1
            worker1.append((count1,t))
        
        #To detect count of worker 2
        if port%3999 == 2:
            count2 += 1
            worker2.append((count2,t))
        
        #To detect count of worker 3
        if port%3999 == 3:
            count3 += 1
            worker3.append((count3,t))

    
    task_match_end = re.search(patTaskEnd,log_data)
    if task_match_end:
        #print(task_match_end.group(1),task_match_end.group(2),task_match_end.group(3))
        t = float(task_match_end.group(1))
        tID = task_match_end.group(2)
        wID = int(task_match_end.group(3))
        if tID in tasks:
            tasks[tID] = t - tasks[tID]
        
        #To decrement count of worker 1 after task completion 
        if wID == 1:
            count1 -= 1
            worker1.append((count1,t))
        
        #To decrement count of worker 2 after task completion 
        if wID == 2:
            count2 -= 1
            worker2.append((count2,t))
        
        #To decrement count of worker 3 after task completion 
        if wID == 3:
            count3 -= 1
            worker3.append((count3,t))
    log_data = logs.readline()

#Printing and generating the required statistical value and graphs
jobs = np.array(list(jobs.values()))
print("Median of job completion time = ", np.median(jobs))
print("Mean of job completion time = ", jobs.mean())
tasks = np.array(list(tasks.values()))
print("Median of task completion time = ", np.median(tasks))
print("Mean of task completion time = ", tasks.mean())
#print(worker1)
#print(worker2)
#print(worker3)

#Function to plot the graph for number of task scheduled on worker vs time
def plotFig(worker,s):
    x,y=[],[]
    for i in worker:
        x.append(i[0])
        y.append(i[1])
    plt.plot(y,x,'--bo')
    plt.xlabel("Time")
    plt.ylabel("Number of running tasks")
    plt.title(s)
    plt.show()

plotFig(worker1,"Worker 1")
plotFig(worker2,"Worker 2")
plotFig(worker3,"Worker 3")




