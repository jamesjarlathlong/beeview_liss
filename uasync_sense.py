import uasyncio as asyncio
import json as json
import utime as time
import select
from zbee.zigbee import ZigBee
import gc
import serial
import os
import crc32
import accelreader
import networking
from algorithms import np
from algorithms import basis_pursuit as bp
from algorithms import fourier_basis as ft
import urandom
urandom.seed(int(os.getenv("NODE_ID")))
import ujson

def conj(a):
    return a.real-1j*a.imag
def argmax(complex_list):
    abs_list = list(map(abs, complex_list))
    idx = abs_list.index(max(abs_list))
    return idx, complex_list[idx]
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        ex_time = te-ts
        return ex_time,result
    return timed
def random_word():
    return str(urandom.getrandbits(12))
@timeit
def benchmark1(size):
    def vec(size):
        return [urandom.getrandbits(8)/100 for i in range(size)]    
    mat = (vec(size) for i in range(size))
    v = np.Vector(*vec(size))
    res = v.gen_matrix_mult(mat)
    return
def timecoroutine(method):
    def timed(*args, **kw):
        ts = time.time()
        result = yield from method(*args, **kw)
        te = time.time()
        ex_time = te-ts
        return ex_time,result
    return timed
def append_record(filename,record):
    with open(filename, 'a') as f:
        json.dump(record, f)
        f.write('\n')
def write_stats_to_file(method):
    def writestats(*args):
        stats = yield from method(*args)
        append_record('/etc/init.d/beeview_liss/tx_stats',stats)
        return stats
    return writestats
def timed_gen(gen, user, key=None, node=None):
    ts = []
    while True:
        try:
            t, res = timeit(next)(gen)
            ts.append(t)
            yield res
        except StopIteration:
            break
    print('timed gen**********',user,key,node)
    if key:
        append_record('/etc/init.d/beeview_liss/px_stats',{'type':'reducer','ts':ts,'onkey':key,'job':user})
    else:
        append_record('/etc/init.d/beeview_liss/px_stats',{'type':'mapper','ts':ts,'mapnum':node,'job':user})
class Comm:
    """ a class organising communications for uasync_sense:
    qs, interrupts and serial objects """
    def __init__(self):
        self.queue = asyncio.Queue(maxsize = 4096)
        self.bm_q = asyncio.Queue(maxsize = 16)
        self.fn_queue = asyncio.Queue()
        self.at_queue = asyncio.Queue()
        self.accelq = asyncio.Queue(maxsize = 4096)
        self.output_q = asyncio.Queue() #q for pieced together messages
        self.coro_queue = asyncio.PriorityQueue()
        self.kv_queue = asyncio.KVQueue(maxsize = 512)
        self.sense_queue = asyncio.KVQueue(maxsize = 32)
        self.f_queue = asyncio.Queue()
        # callback for extint
        def cb(line):
            #print('irq called: ', line)
            pass
        #self.extint = pyb.ExtInt('X10', pyb.ExtInt.IRQ_RISING_FALLING, pyb.Pin.PULL_NONE, cb)
        #self.uart = pyb.UART(1,9600, read_buf_len=1024)
        self.uart = serial.Serial('/dev/ttymxc2', 9600)
        self.accel = serial.Serial('/dev/ttymxc1', 230400)
         #allocating 512 bytes to the uart buffer -  alot? maybe but we have ~128 kB of RAM
        self.ZBee = ZigBee(self.uart, escaped =False)
        self.ZBee.send('at', command=b'NO',parameter=b'\x04')
        self.writer = asyncio.ZigbeeStreamWriter(self.ZBee)
        self.zbee_id = 0
        self.ID = int(os.getenv("NODE_ID"))
        print('ID is: ', self.ID)
        self.address_book = {99: b'\x00\x13\xa2\x00@\xdasp',
                            15: b'\x00\x13\xa2\x00AZ\xe8n',17: b'\x00\x13\xa2\x00AZ\xe8s',
                            18: b'\x00\x13\xa2\x00A\x05F\x99',21: b'\x00\x13\xa2\x00A\x05H}',
                            22: b'\x00\x13\xa2\x00A\x05F\xa2',29: b'\x00\x13\xa2\x00A\x05H\x81',
                            31: b'\x00\x13\xa2\x00AZ\xe8(',32: b'\x00\x13\xa2\x00AZ\xea\x94',
                            37: b'\x00\x13\xa2\x00A\x05H\x86',39: b'\x00\x13\xa2\x00A\x05H\x98',
                            40: b'\x00\x13\xa2\x00A\x05F\x9f',41: b'\x00\x13\xa2\x00AZ\xe8\x10',
                            43: b'\x00\x13\xa2\x00AZ\xe8.',44: b'\x00\x13\xa2\x00A\x05H\x9f',
                            46: b'\x00\x13\xa2\x00A\x03o\x01',49: b'\x00\x13\xa2\x00A\x05F\x9d',
                            53: b'\x00\x13\xa2\x00@\xdasc',55: b'\x00\x13\xa2\x00@\xf5\xff\x19',
                            56: b'\x00\x13\xa2\x00A\x05F\x9a',58: b'\x00\x13\xa2\x00A\x05H\x84',
                            60: b'\x00\x13\xa2\x00AZ\xe8&',61: b'\x00\x13\xa2\x00A\x05F\x97',
                            63: b'\x00\x13\xa2\x00A\x05H\x91',64: b'\x00\x13\xa2\x00@\xdasm',
                            68: b'\x00\x13\xa2\x00@\xdasl',69: b'\x00\x13\xa2\x00A\x05H\x8b',
                            95: b'\x00\x13\xa2\x00@\xdas\x95',96: b'\x00\x13\xa2\x00@\xdasd'}
    def inverse_address(self,value):
        book = self.address_book
        return list(book.keys())[list(book.values()).index(value)]
    def id_counter(self):
        if self.zbee_id<255:
            self.zbee_id+=1
        else:
            self.zbee_id = 0
        return bytes([self.zbee_id])

class Q_Item:
    def __init__(self, x):
        self.x = x
    def __gt__(self,other):
        if self.x == other.x:
            """if they are the same just pick the current one"""
            return True       
        elif self.x == "MAP_DONE":
            """make sure MAP_DONE messages go to front of queue"""
            return False           
        elif other.x == "MAP_DONE":
            return True            
        else:
             return self.x > other.x
    def __repr__(self):
        return str(self.x)        

def datetime_diff(dt1, dt2):
    """given two datetime objects, return the difference
    between them in milliseconds"""
    minutes = dt2[5] - dt1[5]
    seconds = dt2[6] - dt1[6]    
    part_seconds = (255-dt2[7]) - (255-dt1[7])
    millis_elapsed = 60*1000*minutes + 1000*seconds + int(1000*(part_seconds/255))
    return millis_elapsed

def handle_stdin(comm,loop):
    """event handler to read incoming messages from serial port
    and place them on the communication queue"""
    waiting = comm.uart.inWaiting()
    data = comm.uart.read(waiting)
    for byt in data:
        try:
            comm.queue.put_nowait( bytes([byt]) )
        except (asyncio.QueueFull, MemoryError) as e:
            print(e,'queue is full')
    del data
    #gc.collect()
    loop.remove_reader(comm.uart.fd) # polling is one-shot so we want to remove and add file descriptor
    #after each time the event handler is triggered
    loop.add_reader(comm.uart.fd, handle_stdin, comm, loop)

def handle_accel(comm, loop):
    """event handler to read incoming acceleration sensor
    data  from serial port and place it on the accel q"""
    while comm.accel.inWaiting()<4095:
        time.sleep(0.05)
        print('waiting for buffer to fill')
    acc = comm.accel.read(4095)
    print('read all available',len(acc))
    for byt in acc:
        try:
            comm.accelq.put_nowait(bytes([byt]))
        except (asyncio.QueueFull, MemoryError) as e:
            print(e,'accelqueue is full')
    del acc
    loop.remove_reader(comm.accel.fd)
    #loop.remove_reader(comm.accel.fd)
    loop.add_reader(comm.accel.fd, handle_accel, comm, loop)
def flatten(L):
    for item in L:
        try:
            yield from flatten(item)
        except TypeError:
            yield item
def unroll(node_list):
    return list(flatten(node_list))
class ControlTasks:
    SLEEP_READY = 0
    BUSY = 1
    BETWEEN_FUNCS = 2
    DEEP_SLEEP = 0
    IDLE_SLEEP = 1
    def __init__(self,loop,comm):
        self.ID = int(os.getenv("NODE_ID")) 
        self.np = np
        self.eventloop = loop
        self.sleep_for = 0#""""was 1- editing for woburn site"""""1
        self.sleep_mode = ControlTasks.IDLE_SLEEP
        self.wait_atleast = {ControlTasks.IDLE_SLEEP: 0.1, ControlTasks.DEEP_SLEEP: 0.1}
        self.in_map = False
        #states
        #current state
        self.state = ControlTasks.SLEEP_READY
        self.comm = comm
        #self.rtc = pyb.RTC()
        self.sleep_handlers = {ControlTasks.IDLE_SLEEP: self.idle_sleep, ControlTasks.DEEP_SLEEP: self.deep_sleep} 
        self.completed_msgs = {}
        self.neighbors = []
        self.shortcut = {"fdd_single":"""
    def sampler(self,node):
        acc = yield from node.testaccel(512)
        return (node.ID,acc)
    def mapper(self,node,d):
        fts = np.fft(d[1]['x'])
        c = lambda d: (d.real,d.imag)
        yield(0,(d[0],c(fts[6])))
    def reducer(self,node,k,vs):
        ws = [complex(*i[1]) for i in vs]
        G = np.spectral_mat(ws)
        eig = np.pagerank(G)
        c = lambda d: (d.real,d.imag)
        ms = [(vs[idx][0],c(el)) for idx,el in enumerate(eig)]
        yield(k,ms)"""}
    def package(self, job_class, time_received, user):
        #======================#sense stage#======================#    
        exec_time, curried_runfunc = self.package_senser(job_class.sampler,
                                                         time_received, user)
        curried_runfunc['sense_nodes'] = unroll(job_class.sensenodes)
        #=======================#map stage#=======================#
        map_func, map_arg = self.package_function(job_class.mapper)
        curried_runfunc['map_func'] = map_func
        curried_runfunc['map_arg'] = map_arg
        curried_runfunc['num_mappers'] = len(job_class.mapnodes)
        curried_runfunc['map_nodes'] = unroll(job_class.mapnodes)
        #======================#reduce stage#=====================#
        reduce_func, reduce_arg = self.package_reducer(job_class.reducer)
        curried_runfunc['reduce_func'] = reduce_func
        curried_runfunc['reduce_arg'] = reduce_arg
        curried_runfunc['reduce_nodes'] = unroll(job_class.reducenodes)
        return exec_time, curried_runfunc
    @asyncio.coroutine
    def accelpacketyielder(self): 
        """coroutine which consumes acceleration data from the accelq
        performs cyclic redundancy checking to identify valid packets
        from the stream of data coming from the accelq. Once a valid packet is 
        found we return formatted triaxial accel data in the form of a list of 3 lists,
        one for each axis, with values in gs"""  
        packet = bytes()
        crc = crc32.CRC32()
        while True:
            while len(packet)<254:
                accel_byte = yield from self.comm.accelq.get()
                packet += accel_byte
            try:
                ##print('checking packet')
                result = crc.check_packet(packet)
                print('got a packet')
                acc = accelreader.packet_to_3accels(result)
                g_acc = list(map(accelreader.int_to_gs, acc))
                return g_acc
            except ValueError:
                ##print('value error')
                packet = packet[1::]#remove the first byte
                ##print('no packet yet')
                
    @asyncio.coroutine
    def testaccel(self, sample_length):
        fname = '/etc/init.d/beeview_liss/192.168.123.{}.json'.format(self.ID)
        print('opening: ',fname)
        gc.collect()
        print(gc.mem_free())
        i=0
        while True:
            try:
                with open(fname) as json_data:
                    d = ujson.loads(json_data.read())
                    yield from asyncio.sleep(0)
                    trimmed = {k:v[0:sample_length] for k,v in d.items()}
                    return trimmed
            except MemoryError:
                gc.collect()
                i+=1 
                trimmed = []
        return trimmed 
    @asyncio.coroutine
    def accel(self, sample_length):
        """Read from the accelerometer a sample of length sample length.
           Accelerometer samples at 2000Hz, so if sample_length = 2000,
           the reading will be for 4 seconds."""
        result = {'x':[],'y':[],'z':[]}     
        self.comm.accel.open()
        self.eventloop.add_reader(self.comm.accel.fd, handle_accel, self.comm, self.eventloop)
        if self.comm.accel.inWaiting() == 0:
            ##print('wrote gimmie')
            self.comm.accel.write('gimmie')
        while len(result['x'])<sample_length:
            packet = yield from self.accelpacketyielder()
            print('len packet: ', len(packet),len(self.comm.accelq._queue),self.comm.accel.inWaiting() )
            result['x'].extend(packet[0::3])
            result['y'].extend(packet[1::3])
            result['z'].extend(packet[2::3])
            ##print('result: ', len(result['x']))
        self.comm.accel.close()
        del self.comm.accelq
        self.comm.accelq = asyncio.Queue(maxsize = 4096)
        return result
        
    def idle_sleep(self, sleep_time): 
        """ put board into standby: need to save to disk the current event loop time, rtc time,"""
        #print('sleep_time is: ', sleep_time)
        time.sleep(sleep_time)
        #trx = self.comm.ZBee.send('tx', data=bytes( json.dumps({'update':'awake'}), 'ascii' ), dest_addr_long=self.comm.address_book['Server'], dest_addr=b'\xff\xfe')
        return 0
  
    def deep_sleep(self, sleep_time):
        """function puts board into sleepmode,for sleep_time milliseconds. Before entering sleepmode we store any
        current state on disk, to be reloaded upon wakeup"""
        if self.in_map or self.comm.queue.qsize()>0: 
            return 0#just make sure the conditions are true - yield from sleep
        #could result in this func being called after a comp task has begun
        coro_q_list = []
        for q_item in self.comm.coro_queue._queue:
            t = q_item[0]
            try:
                source_code = q_item[1]['source']
                requester_ip = q_item[1]['u']
                coro_q_list.append([t,source_code,requester_ip])
            except KeyError:
                print('Key Error: no source code was available for the function')
                pass
        current_state = {}
        current_state['sleep_mode'] = self.sleep_mode        
        if coro_q_list:      
            before_sleep_eventloop = self.eventloop.time()
            current_state['rtc'] = before_sleep_eventloop
            current_state['looptime'] = before_sleep_eventloop
            current_state['coro_q'] = coro_q_list
        try:
            f = open('/etc/init.d/beeview_liss/log/state_before_standby.txt', 'w')    
            f.write(json.dumps(current_state) )
            print(json.dumps(current_state) )      
            f.close()
            print('closed')
        except:
            pass
        time.sleep(sleep_time)
        #trx = self.comm.ZBee.send('tx', data=bytes( json.dumps({'update':'entering deep sleep'}), 'ascii' ), dest_addr_long=self.comm.address_book['Server'], dest_addr=b'\xff\xfe') 
        #trx = self.comm.ZBee.send('tx', data=bytes( json.dumps({'update':'awake'}), 'ascii' ), dest_addr_long=self.comm.address_book['Server'], dest_addr=b'\xff\xfe')
        return 0
    
    
    def sleep(self, sleep_for):
        """ look up what the current sleep_mode setting is
         and call the appropriate sleep for the given time"""
        t =  self.sleep_handlers[self.sleep_mode](sleep_for)
        return t
        
    def package_function(self,mapper):
        """given a mapper function to be executed at exec time, sent
        by user, package this function so it's ready to be added to the q"""
        @asyncio.coroutine
        def run_func(mapp, data):
            self.in_map = True
            for j in mapp(self, data):
                    yield j
                    yield from asyncio.sleep(0.1) #allow to check for input
            self.in_map = False
        #exec(source_code) 
        #curried_runfunc = {'func':run_func,'arg': mapper,'u':user } #a dict because micropython doesnt like currying a generator    
        return run_func, mapper
    
    def package_senser(self, senser, exec_time, user):
        @asyncio.coroutine
        def sense(senss):
            result = yield from senss(self)
            return result
        curried_senser = {'func':sense,'arg': senser,'u':user }
        return exec_time, curried_senser

    def package_reducer(self, reducer):#TODOneed to make sure this is also saved and redone on deep sleep mode
        """given a reducer function package this function so it's ready to be added to the q"""
        @asyncio.coroutine
        def reduce_func(reducc, key,values):
            self.in_map = True
            for j in reducc(self,key,values):
                    yield j
                    yield from asyncio.sleep(0.1) #allow to check for input
            self.in_map = False   
        #exec(reduce_code)
        return reduce_func, reducer#(locals()['reducer'])
        #curried_runfunc = {'reduce_func':reduce_func,'arg': (locals()['reducer']) }    
    def monkey_patch(self,sid_code, job_class):
        monkey_class ="class Monkey:\n   "+sid_code
        exec(monkey_class)
        monkey = locals()['Monkey']
        job_class.sampler = monkey.sampler
        job_class.mapper = monkey.mapper
        job_class.reducer = monkey.reducer
        return job_class
    def class_definer(self, source_code,shortcut = None):
        """given the user specified source code for the
        SenseReduce class, create this class from the string of code"""
        exec(source_code)
        job_class = locals()['SenseReduce']
        if shortcut:
            job_class = self.monkey_patch(shortcut, job_class)
        return job_class()    
    @asyncio.coroutine
    def radio_listener(self):
        while True:
            yield from self.comm.ZBee.wait_read_multipleframes(self.comm.queue,
                                                               self.comm.output_q)
    @timecoroutine
    def send_and_wait(self, byte_chunk, addr, frame_id=None,level=0):
        """given a chunk of a message to send to addr
        send the chunk, and then wait for confirmation that
        the message has sent before returning"""
        if not frame_id:
            frame_id = self.comm.id_counter()
        print('sending frame id: ', frame_id, self.completed_msgs)
        yield from self.comm.writer.network_awrite(byte_chunk, addr, frame_id)
        status,retries = yield from self.check_acknowledged(frame_id)
        success = b'\x00'
        if status == success:
            print('success sending: ', frame_id, byte_chunk,level)
            return retries, level
        else:
            #wait a second first
            yield from asyncio.sleep(0.2+0.2*level)
            print('trying again: ', frame_id)
            retries, level = yield from self.send_and_wait(byte_chunk, addr, frame_id = frame_id,level=level+1)
            return retries,level
    @asyncio.coroutine
    @write_stats_to_file
    def node_to_node(self, message, address):
        self_address = self.comm.address_book[self.comm.ID]
        if address == self_address:
            if 'kv' in message:
                print('node to node: ', message)
                message['u'] = message['u'][2::]#don't need nodeid concat
            if 'res' in message:
                message['res'] = json.loads(message['res'][1])
            self.message_to_queue(message)
        else:
            stats = yield from self.network_awrite_chunked(message, address) 
            stats['msg'] = message 
            return stats
    @asyncio.coroutine
    def network_awrite_chunked(self, buf, addr):
        """
        takes a single message in dict form,and address, chunks it into smaller pieces
        of the message, adds message number and chk for each, converts
        it to bytes and calls network_awrite for each
        """
        def unpacker(a):
            if isinstance(a[1],int):
                return a
            else:
                return unpacker(a[1])
        def unpack(packed):
            return packed[0], unpacker(packed)
        chunks = networking.chunk_data_to_payload(buf)
        ##print('chunks is: ',chunks)
        byteified_msgs = map(networking.json_to_bytes, chunks)
        nodenum = self.comm.inverse_address(addr)
        try:
            rssi = [i for i in self.neighbors if i['target'] ==nodenum][0]['value']
        except:
            rssi = None
        stats = []
        for byte_chunk in byteified_msgs:
            packaged = yield from self.send_and_wait(byte_chunk, addr)
            print('pack: ',packaged)
            t, (retries,level) = unpack(packaged)
            stats.append({'t':t,'retries':retries,'level':level})
        return {'msgize':len(json.dumps(buf,separators=(',',':'))) ,'stats':stats,'rssi':rssi,'node':nodenum}        
         
    def acknowledge(self, msg):
        """on receipt of an ack from the network, add the msg id
           and status to the dictionary of completed_msgs"""
        ##print('acking')
        msg_id = msg['frame_id']
        status = msg['deliver_status']
        retries = msg['retries'][0]
        if msg['id'] == 'tx_status':
            self.completed_msgs[msg_id] = (status,retries)    
        
    @asyncio.coroutine
    def check_acknowledged(self, msg_id):
        """check if the message associated with msg_id was 
        acknowledged. If no ack within 3 seconds, return failed"""
        counter = 0
        while (msg_id not in self.completed_msgs) and (counter<20):
            yield from asyncio.sleep(0.05)
            counter+=1
        try:
            status,retries = self.completed_msgs[msg_id]
            del self.completed_msgs[msg_id]
        except KeyError:
            status = 'timed out'
            retries = None
        return status, retries
    @asyncio.coroutine
    def benchmark(self):
        while True:
            data = yield from self.comm.bm_q.get()
            t, res = benchmark1(data)
            self.most_recent_benchmark = t
            result_tx =  {'res':(1,json.dumps({'t':t})),'u':self.add_id(random_word()+'bnch'+str(data))}
            yield from self.node_to_node(result_tx, self.comm.address_book[99])

    @asyncio.coroutine
    def report_neighbours(self):
        while True:
            req = yield from self.comm.fn_queue.get()
            print('got req')
            def srv(num):
                val = 99 if num=='Server' else num
                return str(val)
            def cmp(ne):
                return srv(ne['target'])+'_'+srv(ne['value'])
            neighbors = self.neighbors
            cmped = [cmp(n) for n in neighbors]
            whole_thing = ('.').join(cmped)
            result_tx = {'res':(1,json.dumps({'rs':whole_thing})),'u':self.add_id(random_word()+'rs')}
            yield from self.node_to_node(result_tx, self.comm.address_book[99])
    @asyncio.coroutine
    def at_reader(self):
        while True:
            data = yield from self.comm.at_queue.get()
            if data['command'] == b'FN':
                payload = data['parameter']
                #find neighbor address
                neighbor_node_addr = payload[2:10]
                neighbor_number = self.comm.inverse_address(neighbor_node_addr)
                #find rssi
                rssi = payload[-1]
                upsert_data = {'source':self.ID,'target':neighbor_number,'value':rssi}
                self.upsert(upsert_data)
                print('upserting')
    def upsert(self, entry):
        def uq(e):
            return str(e['source'])+str(e['target'])
        uqed = [uq(i) for i in self.neighbors]
        try:
            idx = uqed.index(uq(entry))
            self.neighbors[idx] = entry
        except ValueError:
            self.neighbors.append(entry)
    @asyncio.coroutine
    def find_neighbours(self):
        while True:
            self.get_fn()
            yield from asyncio.sleep(600)
    def get_fn(self):
        self.neighbors = []
        print('sending')
        self.comm.ZBee.send('at', command=b'FN')
        print('sent!')
    def f_to_queue(self, data):
        self.comm.f_queue.put_nowait(data)
    def s_to_queue(self, data): 
        self.comm.sense_queue.put_nowait(data['s'], data['u'])
    def bm_to_queue(self, data):
        self.comm.bm_q.put_nowait(data['bm'])
    def fn_to_queue(self, data):
        self.comm.fn_queue.put_nowait(data['fn'])
    def kv_to_queue(self, data):
        kv_pair = data['kv'] 
        self.comm.kv_queue.put_nowait((Q_Item(kv_pair[0]), kv_pair[1]), data['u'])
    def message_to_queue(self, data):
        queue_map = {'f':self.f_to_queue,
                     'kv':self.kv_to_queue,
                     's':self.s_to_queue
                     ,'bm':self.bm_to_queue
                     ,'fn':self.fn_to_queue}
        matches = [k for k in data if k in queue_map]
        for key in matches:
            queue_map[key](data)    
    @asyncio.coroutine
    def queue_placer(self):
        """consume messages from output q and put it
        on the appropriate q depending on the data type,
        kv, s, or f"""
        while True:
            msg = yield from self.comm.output_q.get()
            print('got a message', msg)
            try:
                data = json.loads(msg['rf_data'])
                self.message_to_queue(data)
            except (KeyError, ValueError) as e:##no rf_data
                if msg['id'] == 'tx_status':
                    self.acknowledge(msg)  
                if msg['id'] in ['at_response', 'remote_at_response']:
                    self.comm.at_queue.put_nowait(msg)  
                #add handling of AT command responses here e.g. finding neighbors    
    @asyncio.coroutine    
    def function_definer(self):
        """consumes function definitions from the f_queue,
        defines them, and puts them on the coro queue"""
        while True:
            data = yield from self.comm.f_queue.get()
            print('got a function: ', data)
            time_received = self.eventloop.time()
            class_def = data['f']
            if isinstance(class_def, tuple) or isinstance(class_def,list):
                sid = class_def[1]
                class_def = class_def[0]
            else:
                sid = None
            user = data['u']
            shortcut_code = self.shortcut.get(sid)
            job_class = self.class_definer(class_def,shortcut =shortcut_code)
            try:
                repeat = job_class.repeat
            except AttributeError:
                repeat = 0
            try:
                every = job_class.every
            except:
                every = 0
            exec_time, curried_runfunc = self.package(job_class, time_received, user)
            #put the coroutine on the queue, as many times as neccesary- first one straight away presumably
            i = 0
            self.comm.coro_queue.put_nowait((exec_time, curried_runfunc))
            print('put it on the q', user)
            while i<repeat:
                exec_time = time_received+(i+1)*every
                curried_runfunc['source'] = class_def
                # store the source code in case we enter standby before execution
                yield from self.comm.coro_queue.put((exec_time, curried_runfunc))
                i+=1
    def add_id(self, user):
        return str(self.comm.ID)+user
    @asyncio.coroutine
    def worker(self):
        """
        """
        while True:
            coro = yield from self.comm.coro_queue.get()     
            if coro[0] < self.eventloop.time():
                self.in_map = True
                curried_func = coro[1]
                if self.comm.ID in curried_func['sense_nodes']:
                    print('starting sampler')
                    yield from self.sense_worker(curried_func)
                    print('finished sampler')
                if self.comm.ID in curried_func['map_nodes']:
                    maps = curried_func['map_nodes']
                    positions = [i for i,e in enumerate(maps) if e==self.comm.ID]
                    print('positions: ',positions)
                    for idx in positions:
                        print('starting mapper',idx)
                        data = yield from self.comm.sense_queue.get(str(idx)+'_'+curried_func['u'])
                        print('got data')
                        yield from self.map_worker(curried_func, data, idx)
                        print('finished mapper')
                if self.comm.ID in curried_func['reduce_nodes']:
                    print('reducing')
                    yield from self.reduce_worker(curried_func)
                    print('reduced')
                self.in_map = False                                                   
            else:   
                yield from self.comm.coro_queue.put(coro)
                yield from asyncio.sleep(0)
    @asyncio.coroutine
    def sense_worker(self, curried_func):
        map_idx = curried_func['sense_nodes'].index(self.comm.ID)
        data = yield from curried_func['func'](curried_func['arg'])
        message = {'s':data, 'u':str(map_idx)+'_'+curried_func['u']}
        child_node = curried_func['map_nodes'][map_idx]
        yield from self.node_to_node(message, self.comm.address_book[child_node])
    def partitioner(self, key, reducer_ids):
        idx = hash(key)%len(reducer_ids)
        return reducer_ids[idx]
    @asyncio.coroutine
    def map_worker(self, curried_func, map_data, map_idx):
        reduce_nodes= curried_func['reduce_nodes']
        job_nodeid = self.add_id(curried_func['u']) 
        gen = curried_func['map_func'](curried_func['map_arg'], map_data) #generator function
        for j in timed_gen(gen, curried_func['u'], node=map_idx):
            print('j is: ',j)
            if isinstance(j, asyncio.Sleep):
                yield j
            else:
                if j is not None:
                    key,value = j
                    dest_id = self.partitioner(key, reduce_nodes)
                    reduce_dest = self.comm.address_book[dest_id]
                    result = {'kv':j, 'u':job_nodeid}
                    yield from self.node_to_node(result, reduce_dest)
            # notify end of map function to reduce node
            #end_message = bytes(json.dumps ( {'kv_pair':("MAP_DONE",0), 'u':curried_func['u']} ), 'ascii')
        reduce_node_addresses = [self.comm.address_book[i] for i in reduce_nodes]
        for dest in reduce_node_addresses:
            end_message = {'kv':("MAP_DONE",random_word()), 'u':job_nodeid}
            yield from self.node_to_node(end_message, dest)
        self.comm.sense_queue.remove(str(map_idx)+'_'+curried_func['u'])       
        return
    #@asyncio.coroutine
    def get_groupedby(self,q):
        first_pair = q.get_nowait()
        key = first_pair[0].x
        list_kv_pairs = []   
        list_kv_pairs.append(first_pair[1])          
        #list_kv_pairs[key] = [ first_pair[1] ]
        try:
            next_pair = q.get_nowait()
            while next_pair[0].x == key:
                list_kv_pairs.append( next_pair[1] )
                next_pair = q.get_nowait()
            #put the first pair with a different key back on the q
            q.put_nowait(next_pair)
        except asyncio.QueueEmpty as e:
            return list_kv_pairs,key
        return list_kv_pairs,key
    
    @asyncio.coroutine
    def reduce_worker(self,curried_func):
        user = curried_func['u']
        reduce_controller = curried_func['reduce_func']
        reduce_logic = curried_func['reduce_arg']
        num_mappers = curried_func['num_mappers']
        num_reducers = len(curried_func['reduce_nodes'])
        i = 0
        #check if all num_mappers have given a map_done message
        results = {}
        already_seen = set()
        while i<num_mappers:
            yield from asyncio.sleep(0.05)
            kv = yield from self.comm.kv_queue.get(user)
            if kv[0].x == "MAP_DONE":
                ##print('got map done from ',i)
                if kv[1] not in already_seen:  
                    i+=1
                    already_seen.add(kv[1])
            else:
                yield from self.comm.kv_queue.put(kv, user)
        #getting here means all map functions are finished- q size wont change in this function
        while self.comm.kv_queue.qsize(user):
            grouped_pairs, key = self.get_groupedby(self.comm.kv_queue._qs[user]) #, first_pair)
            #key = first_pair[0].x
            values = grouped_pairs
            reduce_gen = reduce_controller(reduce_logic, key, values)
            #now advance the generator
            for j in timed_gen(reduce_gen, user, key=key):
                    if isinstance(j, asyncio.Sleep):
                        yield j
                    else:
                        if j is not None:
                            ##print('got a result from reducer! ', j)
                            results[key] = j[1]
        #reduce is now finished on all pairs of key, values
        result_tx =  {'res':(num_reducers,nested_json_dump(results)),'u':self.add_id(curried_func['u'])}
        print('reduce finished: ', result_tx)
        #return to sink node
        yield from self.node_to_node(result_tx, self.comm.address_book[99])
        print('map-reduce finished')
        self.comm.kv_queue.remove(user) 
        return
                
    @asyncio.coroutine
    def allow_read_data(self):
        ##print('allowing event loop control', self.comm.uart.inWaiting(), self.in_map)
        yield from asyncio.sleep(0.2)#make sure we have time to read from data queue      
    
    @asyncio.coroutine
    def sleep_until_scheduled(self):
        until_next_epoch = self.comm.coro_queue._queue[0][0] - self.eventloop.time()
        #check to make sure it's not negative:
        if until_next_epoch > 0:
        #pyb.delay( min(until_next_epoch,self.sleep_for) ) 
            #yield from asyncio.sleep(self.wait_atleast[self.sleep_mode])
            #print('going to sleep for: ', min(until_next_epoch,self.sleep_for))
            time_asleep = self.sleep(min(until_next_epoch,self.sleep_for))
            self.eventloop.increment_time(time_asleep) 
        yield from asyncio.sleep(0)
    
    @asyncio.coroutine
    def regular_sleep(self):  
        #pyb.delay(self.sleep_for) #this is how long we sleep for
        #print('going to rsleep for: ', self.sleep_for)
        time_asleep = self.sleep(self.sleep_for)
        #self.eventloop.increment_time(time_asleep)
        yield from asyncio.sleep(self.wait_atleast[self.sleep_mode])
         #exit so we can check messages
    @asyncio.coroutine
    def sleep_manager(self):
        handler_funcs = {ControlTasks.BUSY: self.allow_read_data,
                     ControlTasks.BETWEEN_FUNCS: self.sleep_until_scheduled,
                     ControlTasks.SLEEP_READY: self.regular_sleep}         
        while True:
            # check state
            if (self.comm.queue.qsize()>0 or self.comm.kv_queue.num_qs()>0 or
                self.in_map or self.comm.uart.inWaiting()>0):
                self.state = ControlTasks.BUSY
            elif self.comm.coro_queue.qsize()>0:
                self.state = ControlTasks.BETWEEN_FUNCS
            else:
                self.state = ControlTasks.SLEEP_READY
            # call appropriate coroutine for to handle current state
            yield from handler_funcs[self.state]()
def nested_json_dump(d):
    return json.dumps({json.dumps(k):v for k,v in d.items()})            
@asyncio.coroutine
def loop_stopper():
    while True:
        ##print('in loop stopper stop')
        yield from asyncio.stop()
            
def initialise():
    comm = Comm()
    #trx = comm.ZBee.send('tx', data= bytes( json.dumps({'update':'awake'}), 'ascii' ),
    #                      dest_addr_long=comm.address_book['Server'], dest_addr=b'\xff\xfe')
    loop = asyncio.get_event_loop()
    controller = ControlTasks(loop, comm)
    loop.add_reader(comm.uart.fd, handle_stdin, comm, loop)
    # check for previous state before standby
    try:
        f = open('/etc/init.d/beeview_liss/log/state_before_standby.txt', 'r')
        state_dict = json.loads(f.read())
        f.close()
    except:
        state_dict = {}
    # trx = comm.ZBee.send('tx', data=bytes(json.dumps(len(state_dict)), 'ascii'), dest_addr_long=comm.addr, dest_addr=b'\xff\xfe')
    try:
        controller.sleep_mode = state_dict['sleep_mode']
    except KeyError:
        print('KeyError: no sleep_mode on first initialisation')
    try:
        rtc_before = state_dict['rtc']
        loop_before = state_dict['looptime']
        #rtc_after = controller.rtc.datetime()
        #diff = datetime_diff(rtc_before,rtc_after)
        #controller.eventloop.increment_time(diff+loop_before)
        coro_q_list = state_dict['coro_q']
        for func_item in coro_q_list:
            exec_time = func_item[0]
            source = func_item[1]
            user = func_item[2]
            job_class = controller.class_definer(source)
            t, curried_runfunc = controller.package(job_class, exec_time, user)
            comm.coro_queue.put_nowait((t, curried_func))
            # trx = comm.ZBee.send('tx', data=bytes(json.dumps({'exec time':exec_time}), 'ascii'), dest_addr_long=comm.addr, dest_addr=b'\xff\xfe')
            # trx = comm.ZBee.send('tx', data=bytes(json.dumps({'loop time':controller.eventloop.time()}), 'ascii'), dest_addr_long=comm.addr, dest_addr=b'\xff\xfe')        
    except KeyError:
        print('KeyError: State information was empty')
    return comm, controller                    
def main(): 
    comm, controller = initialise()
    tasks = [controller.radio_listener(), controller.queue_placer(), controller.benchmark(),
             controller.function_definer(), controller.worker(), controller.sleep_manager()
             ,controller.find_neighbours(),controller.report_neighbours(), controller.at_reader()]
    for task in tasks:
        controller.eventloop.call_soon(task)
    #controller.eventloop.call_later(1800, loop_stopper())
    controller.eventloop.run_forever()
    controller.eventloop.close()
    trx = comm.ZBee.send('tx', data=bytes( json.dumps({'update':'exiting'}), 'ascii' ),
                         dest_addr_long=comm.address_book[99], dest_addr=b'\xff\xfe')
    return
if __name__ == '__main__':
    main()
