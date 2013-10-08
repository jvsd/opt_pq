from rq import Connection, Queue, use_connection
from redis import Redis
import zmq
import time
import numpy as np
import job


class Optimizer():

    def __init__(self,zmq_cont,ip='127.0.0.1',port=6000):
        
        self.zmq_cont = zmq_cont

        ## M - Machines by N queues Q = (M,N) 


        use_connection()
        #setup queues

        self.listen = self.zmq_cont.socket(zmq.SUB)
        #fix for dist network
        self.listen.setsockopt(zmq.SUBSCRIBE,'')
        self.listen.connect('tcp://127.0.0.1:' + str(port+1))

        self.pinger = self.zmq_cont.socket(zmq.PUB)
        self.pinger.bind('tcp://*:' + str(port))
        print 'Pinging...'

        #Important thigns...
        self.machines = []
        self.queues = []
        self.queue_names = []

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

    def send_state(self,next_state):
        '''Sends the state matrix to the network'''
        #check assumptions
        if len(self.machines) != self.state.shape[0] or len(self.queues) != self.state.shape[1]:
            print 'machine, queue, state mis-match'
            return False
        for m in range(self.state.shape[0]):
            machine_name = self.machines[m]
            msg = machine_name
            for n in range(self.state.shape[1]):
                msg = msg + ',' + self.queue_names[n] + ',' + str(self.state[m][n])
            self.pinger.send(msg)

        return True

    def set_queues(self, queue_in):
        queues = []
        queue_names = []
        for index,item in enumerate(queue_in):
            queues.append(Queue(item))
            queue_names.append(item)

        self.queues = queues
        self.queue_names = queue_names
    def init_state(self,state_in):
        self.state = np.zeros((len(self.machines),len(self.queues)))+state_in
        

def some_job():
    time.sleep(5)

if __name__=='__main__':
    zmq_cont = zmq.Context()
    optimizer = Optimizer(zmq_cont)
    time.sleep(.5)
    optimizer.machines = optimizer.find_machines()
    print optimizer.machines

    #Add some queues...
    optimizer.set_queues(['q1','q2','q3'])
    optimizer.init_state(2)
    optimizer.send_state(optimizer.state)

    raw_input('Waiting to queue jobs...')

    t1 = time.time()
    for q in range(len(optimizer.queues)):
        for i in range(10):
           j = optimizer.queues[q].enqueue(job.fibonacci,32)
    while(j.result == None):
        time.sleep(.001)
    print time.time()-t1



