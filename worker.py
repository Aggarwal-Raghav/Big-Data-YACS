import sys
import socket
import numpy
import json
import threading
import time
import random
import logging

def createLogHandler(log_file):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

log_file = 'logs.log'
logger = createLogHandler(log_file)
#logger.info('Logger has been created')

#logging.basicConfig(filename='logs.log',level=logging.INFO)

port = int(sys.argv[1])
Id = sys.argv[2]

def taskRun(taskData):
    dur = int(taskData['duration'])
    time.sleep(dur)                                                 # sleep -> task running simulation
    dataToSend =  taskData['task_id'] + " " +Id                     # eg: "0_M0 1"
    sendToMaster(dataToSend)

def workerListen():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverAddress = ("localhost", port)
    sock.bind(serverAddress)
    sock.listen(1)

    while 1:
        connection, address = sock.accept()
        data = connection.recv(1024).decode("utf-8")
        taskData = json.loads(data)                                         #eg: {'task_id' : "0_M0", 'duration':3}
        logger.info(str(time.time())+": Sending Task request to Worker on port :"+str(port)+": with task_id :"+taskData['task_id'])
        threadCreate = threading.Thread(target = taskRun, args=(taskData,))
        threadCreate.start()
    connection.close()

def sendToMaster(ID):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 5001))
        message=json.dumps(ID)
        logger.info(str(time.time())+": Completed task with ID :"+ID)
        s.send(message.encode())
        print("Completed Job. Sending with return ID : ",ID)
        s.close()

print("Worker ID",Id,"On port",port)

thread1 = threading.Thread(target = workerListen)
thread1.start()