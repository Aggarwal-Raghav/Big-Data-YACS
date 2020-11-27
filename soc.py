import socket
import sys
import json
import threading
import time

sem = threading.Semaphore()
LIST=[]

def rec_request():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ("localhost", 5000)
    sock.bind(server_address)
    sock.listen(1)

    while 1:
        connection , address = sock.accept()
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

rec_request()
print('hello')
def display():
    sem.release()
    print(*LIST)
    sem.acquire()
"""
thread1 = threading.Thread(target = rec_request)
thread1.start()

thread2 = threading.Thread(target = display)
thread2.start()

thread1.join()
thread2.join()
"""
