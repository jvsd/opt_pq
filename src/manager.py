from rq import Connection, Queue
from redis import Redis
import zmq
import time


class Manager:
    def __init__(self,machine_name,ip='127.0.0.1',port=6000):
        self.zmq_cont = zmq.Context()
        self.redis_conn = Redis()
        self.opt_recv = self.zmq_cont.socket(zmq.SUB)
        self.opt_recv.setsockopt(zmq.SUBSCRIBE,'')
        self.opt_recv.connect('tcp://' + ip + ':' + str(port))
        self.opt_send = self.zmq_cont.socket(zmq.PUB)
        #fix for dist network
        self.opt_send.bind('tcp://*:'+str(port+1))
        self.machine_name = machine_name

    def subscribe_machines(self):
        msg = self.opt_recv.recv()
        print 'Received:' + msg
        self.opt_send.send(self.machine_name)
        time.sleep(1)
        self.opt_recv.setsockopt(zmq.SUBSCRIBE,self.machine_name)


if __name__ == '__main__':
    man = Manager('man1')
    print 'Waiting for optimizer...'
    man.subscribe_machines()
