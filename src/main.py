from rq import use_connection, Queue
import optimizer
import zmq
import time
import job

#3 tier application
use_connection()

q1 = Queue('q1')
q2 = Queue('q2')
q3 = Queue('q3')

zmq_cont = zmq.Context()
opt = optimizer.Optimizer(zmq_cont)
opt.machines = opt.find_machines()

opt.set_queues(['q1','q2','q3'])
opt.init_state(2)
opt.send_state(opt.state)

args = [[.3,.5,1],['q3','q2']]

q1.enqueue(job.job,args)
