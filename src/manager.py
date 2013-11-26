from rq import Connection, Queue, Worker, use_connection
import rq as rq
from redis import Redis
import zmq
import time
from multiprocessing import Process
from threading import Thread
import job
#Optional preload libraries

class Manager:
    def __init__(self,machine_name,ip='127.0.0.1',port=6000):
        self.zmq_cont = zmq.Context()
        self.redis = Redis('127.0.0.1',6379)
        use_connection(self.redis)
        self.opt_recv = self.zmq_cont.socket(zmq.SUB)
        self.opt_recv.setsockopt(zmq.SUBSCRIBE,'')
        self.opt_recv.connect('tcp://' + ip + ':' + str(port))
        self.opt_send = self.zmq_cont.socket(zmq.REQ)
        #fix for dist network
        self.opt_send.connect('tcp://'+ip+':'+str(port+1))
        self.machine_name = machine_name
        #manager state is a Dictionary mapping queues to number of workers.
        self.state = {}
        #workers is a dictionary mapping queue to lists of workers
        self.workers = {}
        #Dictionary{'queue name':rq.Queue}
        self.queues = {}

    def subscribe_machines(self):
        self.opt_send.send(self.machine_name)
        poller = zmq.Poller()
        poller.register(self.opt_send,zmq.POLLIN)
        socks = dict(poller.poll(2000))
        if self.opt_send in socks and socks[self.opt_send] == zmq.POLLIN:
            msg = self.opt_send.recv()
            if msg == 'ack':
                self.opt_recv.setsockopt(zmq.SUBSCRIBE,self.machine_name)
                return True
                      

    def set_state(self,next_state):
        ''' when self.state is set it will set state ''' 
        if next_state != self.state:
            for key in next_state:
                if key not in self.queues:
                    self.queues[key] = Queue(key)
                    self.state[key] = 0
                    self.workers[key] = []
                changed = int(self.state[key])-int(next_state[key])
                if changed < 0:
                    for i in range(-changed):
                        #Needed depending on process or thread
                        #worker_name = key + str(int(len(self.workers[key]))+1)
                        self.workers[key].append(Process(target=Worker,args = (self.queues[key],None,500,self.redis,None,420)))
                        temp_length = len(self.workers[key])
                        self.workers[key][temp_length-1].start()
                        self.state[key] +=1
                if changed > 0:
                    for i in range(changed):
                        w = self.workers[key].pop()
                        w.terminate()
                        self.state[key] -=1
            
    def recv_state(self):
        msg = self.opt_recv.recv()
        lmsg = msg.split(',')
        state = {}
        for i in range(int((len(lmsg)-1)/2.)):
            #temp_list.append((lmsg[2*i+1],int(float(lmsg[2*i+2]))))
            key, value = lmsg[2*i+1], int(float(lmsg[2*i+2]))
            state[key] = value
        return state



if __name__ == '__main__':
    manager = Manager('man1')
    print 'Waiting for optimizer...'
    manager.subscribe_machines()
    next_state = manager.recv_state()
    manager.set_state(next_state)
    while(True):
        next_state = manager.recv_state()
        manager.set_state(next_state)
        print manager.workers
        time.sleep(1)


