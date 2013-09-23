from redis import Redis
from rq import Queue

q = Queue(connection=Redis())

# Import the job to enqueue
import job
for i in range(30):
    result = q.enqueue(job.fibonacci, i)
