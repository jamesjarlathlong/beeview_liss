import errno
import select
import ujson as json
import networking
from uasyncio.core import *
from uasyncio.queues import Queue, QueueEmpty,QueueFull, PriorityQueue, KVQueue

class callback_poll():  
    def __init__(self):
        self.poller = select.epoll(1)
        self.registry = {}      
    def register(self, fd, eventmask, cb,*arg ):
        self.poller.register(fd, eventmask)
        self.registry[str(fd)] = cb
        print('registry is: ', self.registry) 
    def unregister(self,fd):
        self.poller.unregister(fd)
        print('in unregister: ', self.registry)

        del self.registry[str(fd)]       
    def poll(self,delay):
        res = self.poller.poll(delay)
        if res:
            for cb, ev in res:
                cb = self.registry[str(cb)]
            res = [(cb,ev)]

        return res       
class EpollEventLoop(EventLoop):

    def __init__(self):
        EventLoop.__init__(self)
        self.poller = select.epoll(1)
        self.objmap = {}

    def add_reader(self, fd, cb, *args):
        #if __debug__:
        #    log.debug("add_reader%s", (fd, cb, args))
        if args:
            self.poller.register(fd, select.EPOLLIN)
            self.objmap[fd] = (cb, args)
        else:
            self.poller.register(fd, select.EPOLLIN)
            self.objmap[fd] = cb

    def remove_reader(self, fd):
        #if __debug__:
        #    log.debug("remove_reader(%s)", fd)
        self.poller.unregister(fd)
        del self.objmap[fd]

    def add_writer(self, fd, cb, *args):
        #if __debug__:
        #    log.debug("add_writer%s", (fd, cb, args))
        if args:
            self.poller.register(fd, select.POLLOUT)
            self.objmap[fd] = (cb, args)
        else:
            self.poller.register(fd, select.POLLOUT)
            self.objmap[fd] = cb

    def remove_writer(self, fd):
        #if __debug__:
        #    log.debug("remove_writer(%s)", fd)
        try:
            self.poller.unregister(fd)
            self.objmap.pop(fd, None)
        except OSError as e:
            # StreamWriter.awrite() first tries to write to an fd,
            # and if that succeeds, yield IOWrite may never be called
            # for that fd, and it will never be added to poller. So,
            # ignore such error.
            if e.args[0] != errno.ENOENT:
                raise

    def wait(self, delay):
        #if __debug__:
        #    log.debug("epoll.wait(%d)", delay)
        # We need one-shot behavior (second arg of 1 to .poll())
        if delay == -1:
            res = self.poller.poll(-1)
        else:
            res = self.poller.poll(int(delay * 1000))
        #log.debug("epoll result: %s", res)
        for fd, ev in res:
            cb = self.objmap[fd]
            #if __debug__:
             #   log.debug("Calling IO callback: %r", cb)
            if isinstance(cb, tuple):
                cb[0](*cb[1])
            else:
                self.call_soon(cb)




import uasyncio.core

uasyncio.core._event_loop_class = EpollEventLoop
class StreamWriter:
    def __init__(self, s):
        self.s = s
    def awrite(self, buf):
        # This method is called awrite (async write) to not proliferate
        # incompatibility with original asyncio. Unlike original asyncio
        # whose .write() method is both not a coroutine and guaranteed
        # to return immediately (which means it has to buffer all the
        # data), this method is a coroutine.
        sz = len(buf)
        if __debug__:
            log.debug("StreamWriter.awrite(): spooling %d bytes", sz)
        while True:
            res = self.s.write(buf)
            # If we spooled everything, return immediately
            if res == sz:
                if __debug__:
                    log.debug("StreamWriter.awrite(): completed spooling %d bytes", res)
                return
            if res is None:
                res = 0
            if __debug__:
                log.debug("StreamWriter.awrite(): spooled partial %d bytes", res)
            assert res < sz
            buf = buf[res:]
            sz -= res
            s2 = yield IOWrite(self.s)
            #assert s2 == self.s
            if __debug__:
                log.debug("StreamWriter.awrite(): can write more")

    def close(self):
        yield IOWriteDone(self.s)
        self.s.close()

    def __rescpr__(self):
        return "<StreamWriter %r>" % self.s
        
class ZigbeeStreamWriter(StreamWriter):
    def __init__(self, s):
        self.s = s
         
    def network_awrite_single(self, single, addr,frame_id):
        """
        takes a single message in dict form, converts it to bytes
        and calls network_awrite
        """
        byteified = networking.json_to_bytes(single)
        yield from self.network_awrite(byteified, addr,frame_id)
                   
    def network_awrite(self, buf, addr, frame_id):
        # This method subclasses streamwriter.awrite method to allow
        # networking addressing and zigbee specific protocol
        frame = self.s.prepare_send('tx', frame_id = frame_id, data=buf,
                                    dest_addr_long=addr, dest_addr=b'\xff\xfe')        
        sz = len(frame)
        while True:
            res = self.s.serial.write(frame)
            #print('res is: ', res, 'sz is: ', sz)
            # If we spooled everything, return immediately
            if res == sz:
                return
            if res is None:
                res = 0
            assert res < sz
            buf = buf[res:]
            sz -= res 
            s2 = yield IOWrite(self.s.serial)
            #assert s2 == self.s              
print('init finished')
