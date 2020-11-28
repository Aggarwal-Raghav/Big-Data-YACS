import socket
import sys
import json
import threading
import time

sem = threading.Semaphore()
LIST=[]

path_conf = sys.argv[1]
conf = open(path_conf,'r').read()



def rec_request():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ("localhost", 5000)
    sock.bind(server_address)
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

def display():
    while 1:
        sem.acquire()
        print(*LIST)
        sem.release()

thread1 = threading.Thread(target = rec_request)
thread1.start()


thread2 = threading.Thread(target = display)
thread2.start()

thread1.join()
thread2.join()




def master_worker():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ("localhost", 5001)
    sock.bind(server_address)
    sock.listen(1)

    while 1:
        connection, address = sock.accept()
        data = connection.recv(2048)
        if data==b'':
            sem.release()
            break
        obj = json.loads(data.decode("utf-8"))
    
        print(obj)
    connection.close()

