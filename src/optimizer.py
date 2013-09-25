from rq import Connection, Queue
from redis import Redis
import zmq
import time


class Optimizer():

    def __init__(self,zmq_cont,ip='127.0.0.1',port=6000):
        
        self.zmq_cont = zmq_cont

        ## M - Machines by N queues Q = (M,N) 


        self.redis_conn = Redis()
        #setup queues
        self.distort_q = Queue('distort',connection=self.redis_conn)
        self.chip_q = Queue('chip',connection=self.redis_conn)
        self.recog_q = Queue('recog',connection=self.redis_conn)

        self.listen = self.zmq_cont.socket(zmq.SUB)
        #fix for dist network
        self.listen.setsockopt(zmq.SUBSCRIBE,'')
        self.listen.connect('tcp://127.0.0.1:' + str(port+1))

        self.pinger = self.zmq_cont.socket(zmq.PUB)
        self.pinger.bind('tcp://*:' + str(port))
        print 'Pinging...'

    def find_machines(self,port=6000):
        '''Find machines on port P, server must be listening for replies on port P+1, returns machines replies in a list'''
        self.pinger.send('ping')
        poller = zmq.Poller()
        poller.register(self.listen, zmq.POLLIN)
        timeout = 0
        machines = []
        while(timeout < 1):
            socks = dict(poller.poll(0))
            if self.listen in socks and socks[self.listen] == zmq.POLLIN:
                machines.append(self.listen.recv(0))
            time.sleep(.1)
            timeout+=.1
            print 'Timeout: ' + str(timeout)
        return machines
if __name__=='__main__':
    zmq_cont = zmq.Context()
    opt = Optimizer(zmq_cont)
    time.sleep(.5)
    opt.machines = opt.find_machines()
    print opt.machines





