import re
import numpy as np
import matplotlib.pyplot as plt

patJobStart = '^INFO:root:([0-9.]*):.*:(\d*)$'
patTaskStart = '^INFO:root:([0-9.]*):.*:(\d*):.*:(.*)$'
patTaskEnd = '^INFO:root:([0-9.]*):.*:(\S*) (\d)$'

logs = open('logs.log','r')
log_data = logs.readline()

jobs = dict()
tasks = dict()
worker1 = []
count1 = 0
worker2 = []
count2 = 0
worker3 = []
count3 = 0

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
        if port%3999 == 1:
            count1 += 1
            worker1.append((count1,t))
        if port%3999 == 2:
            count2 += 1
            worker2.append((count2,t))
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
        if wID == 1:
            count1 -= 1
            worker1.append((count1,t))
        if wID == 2:
            count2 -= 1
            worker2.append((count2,t))
        if wID == 3:
            count3 -= 1
            worker3.append((count3,t))
    log_data = logs.readline()

#print(jobs)
jobs = np.array(list(jobs.values()))
print("Median of job completion time = ", np.median(jobs))
print("Mean of job completion time = ", jobs.mean())
tasks = np.array(list(tasks.values()))
print("Median of task completion time = ", np.median(tasks))
print("Mean of task completion time = ", tasks.mean())
print(worker1)
print(worker2)
print(worker3)