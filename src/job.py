# A long running job that should be run with RQ
from rq import use_connection, Queue
import time

def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

def job_sleep(sleep_time):
    time.sleep(float(sleep_time))
    print 'slept'
    return True

def job(args):
    #[Sleep Time], [Queues], 
    use_connection()
    tsleep = args[0].pop()
    time.sleep(float(tsleep))
    if len(args[1])>1:
        current_queue = args[1].pop()
        print current_queue
        q = Queue(str(current_queue))
        q.enqueue(job,args)
    return True
