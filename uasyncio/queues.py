#from deque import deque
from uasyncio.core import sleep
import uheapq as heapq
import gc

class QueueEmpty(Exception):
    """Exception raised by get_nowait()."""


class QueueFull(Exception):
    """Exception raised by put_nowait()."""


class Queue:
    """A queue, useful for coordinating producer and consumer coroutines.
    If maxsize is less than or equal to zero, the queue size is infinite. If it
    is an integer greater than 0, then "yield from put()" will block when the
    queue reaches maxsize, until an item is removed by get().
    Unlike the standard library Queue, you can reliably know this Queue's size
    with qsize(), since your single-threaded uasyncio application won't be
    interrupted between calling qsize() and doing an operation on the Queue.
    """
    _attempt_delay = 0.3

    def __init__(self, maxsize=512):
        self.maxsize = maxsize
        self._queue = [None]*maxsize
        self._firstNone = 0

    def _get(self):
        firstItem = self._queue.pop(0)
        self._firstNone -= 1
        self._queue.append(None)
        return firstItem#left()

    def get(self):
        """Returns generator, which can be used for getting (and removing)
        an item from a queue.
        Usage::
            item = yield from queue.get()
        """
        while not self.qsize():
            #print('nothing on the q')  
            yield from sleep(self._attempt_delay)
        return self._get()

    def get_nowait(self):
        """Remove and return an item from the queue.
        Return an item if one is immediately available, else raise QueueEmpty.
        """
        #if not self._queue:
        if not self.qsize():
            raise QueueEmpty()
        return self._get()
                
    def _put(self, val):
        #first_None = next(index for index,val in enumerate(self._queue) if val==None)
        self._queue[self._firstNone] = val#.append(val)
        self._firstNone +=1

    def put(self, val):
        """Returns generator which can be used for putting item in a queue.
        Usage::
            yield from queue.put(item)
        """
        while self.qsize() > self.maxsize and self.maxsize:
            print('here for some unknown reason')
            yield from sleep(self._attempt_delay)
        self._put(val)

    def put_nowait(self, val):
        """Put an item into the queue without blocking.
        If no free slot is immediately available, raise QueueFull.
        """
        if self.qsize() >= self.maxsize and self.maxsize:
            raise QueueFull()
        #print(gc.mem_free(), self.qsize(), self.maxsize)
        if gc.mem_free() <10000:
            gc.collect()
        self._put(val)

    def qsize(self):
        """Number of items in the queue."""      
        return self._firstNone #len(self._queue)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return not self._queue

    def full(self):
        """Return True if there are maxsize items in the queue.
        Note: if the Queue was initialized with maxsize=0 (the default),
        then full() is never True.
        """
        if self.maxsize <= 0:
            return False
        else:
            return self.qsize() >= self.maxsize
            
class PriorityQueue(Queue):
    """A subclass of Queue; retrieves entries in priority order (lowest first).
    Entries are typically tuples of the form: (priority number, data).
    """

    def __init__(self, maxsize=512):
        self._queue = []
        self.maxsize = maxsize

    def _put(self, item, heappush=heapq.heappush):
        heappush(self._queue, item)

    def _get(self, heappop=heapq.heappop):
        return heappop(self._queue)
    
    def qsize(self):
        """Number of items in the queue."""      
        return len(self._queue)

class KVQueue():
    """Class that maintains a dictionary of Priority Queues,
    one for each jobid"""
    def __init__(self, maxsize=512):
        self._qs = {}
        self.maxsize = maxsize
    def put(self, item, _id):
        """put item on the corresponding q, if it doesnt exist init it"""
        try:
            yield from self._qs[_id].put(item)
            #print('q is: ',self._qs[_id]._queue)
        except KeyError:
            self._qs[_id] = PriorityQueue(maxsize=self.maxsize)
            ##print('began q: ', item, _id)
            yield from self._qs[_id].put(item)
            ##print(self._qs[_id]._queue)
    def put_nowait(self, item, _id):
        """put_nowait item on the corresponding q, if it doesnt exist init it"""
        try:
            self._qs[_id].put_nowait(item)
        except KeyError:
            self._qs[_id] = PriorityQueue(maxsize=self.maxsize)
            self._qs[_id].put_nowait(item)
    def get(self, _id):
        """get item from the q corresponding to _id"""
        try:
            item = yield from self._qs[_id].get()
            return item
        except KeyError:
            self._qs[_id] = PriorityQueue(maxsize=self.maxsize)
            item = yield from self._qs[_id].get()
            return item
    def qsize(self, _id):
        try:
            return self._qs[_id].qsize()
        except KeyError:
            return 0
    def num_qs(self):
        return len([key for key in self._qs.keys()])
    def remove(self, _id):
        del self._qs[_id]
