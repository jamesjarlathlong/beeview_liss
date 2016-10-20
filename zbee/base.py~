import utime as time
import ustruct as struct
from zbee.frame import APIFrame
from zbee.python2to3 import byteToInt, intToByte
    
class CommandFrameException(KeyError):
    pass

class XBeeBase():
    def __init__(self, ser, shorthand=True, escaped=False):
        super(XBeeBase, self).__init__()
        self.serial = ser
        self.shorthand = shorthand
        self._escaped = escaped  
            
    def _write(self, data):
        #print('data to output is: ', data)
        frame = APIFrame(data, self._escaped).output()
        #print('frame is: ', frame)
        res = self.serial.write(frame)
        return res
        
    
    def _wait_for_frame(self):
        frame = APIFrame(escaped=self._escaped)  
        while True:
                #if not self.serial.any():
                if self.serial.inWaiting() == 0:
                    #print(self.serial.any())
                    time.sleep(0.1)
                    #print('nothing to read')
                    continue
                
                byte = self.serial.read(1)

                if byte != APIFrame.START_BYTE:
                    continue

                # Save all following bytes, if they are not empty
                if len(byte) == 1:
                    frame.fill(byte)
                    
                while(frame.remaining_bytes() > 0):
                    #print('remaining: ', frame.remaining_bytes())
                    byte = self.serial.read(1)
                    #print('read a byte: ', byte, 'len: ', len(byte) )
                    if len(byte) == 1:
                        frame.fill(byte)

                try:
                    #print("trying to return")
                    # Try to parse and return result
                    frame.parse()
                    #print("parsed it")
                    # Ignore empty frames
                    if len(frame.data) == 0:
                        #print("no frame data")
                        frame = APIFrame()
                        continue
                    #print("about to return")    
                    return frame
                except ValueError:
                    print("Bad frame, so restart")
                    frame = APIFrame(escaped=self._escaped)
                        
    def _build_command(self, cmd, **kwargs):
        try:
            cmd_spec = self.api_commands[cmd]
        except AttributeError:
            raise NotImplementedError("API command specifications could not be found; use a derived class which defines 'api_commands'.")
            
        packet = b''
        
        for field in cmd_spec:
            try:
                # Read this field's name from the function arguments dict
                data = kwargs[field['name']]
                #print('data is: ', type(data),field['name'] )
            except KeyError:
                # Data wasn't given
                # Only a problem if the field has a specific length
                if field['len'] is not None:
                    # Was a default value specified?
                    default_value = field['default']
                    if default_value:
                        # If so, use it
                        data = default_value
                        #print('default is: ', type(data))
                    else:
                        # Otherwise, fail
                        raise KeyError(
                            "The expected field %s of length %d was not provided" 
                            % (field['name'], field['len']))
                else:
                    # No specific length, ignore it
                    data = None
            
            # Ensure that the proper number of elements will be written
            if field['len'] and len(data) != field['len']:
                raise ValueError(
                    "The data provided for '%s' was not %d bytes long"\
                    % (field['name'], field['len']))
        
            # Add the data to the packet, if it has been specified
            # Otherwise, the parameter was of variable length, and not 
            #  given
            
            if data:
                #print('data is: ', type(data), data, len(data))
                #print('packet is: ', type(packet), packet)
                packet += data
        #print('final packet is', packet)        
        return packet
    
    def _split_response(self, data):
        packet_id = data[0:1]
        try:
            packet = self.api_responses[packet_id]
        except AttributeError:
            raise NotImplementedError("API response specifications could not be found; use a derived class which defines 'api_responses'.")
        except KeyError:
            # Check to see if this ID can be found among transmittible packets
            for cmd_name, cmd in list(self.api_commands.items()):
                if cmd[0]['default'] == data[0:1]:
                    raise CommandFrameException("Incoming frame with id %s looks like a command frame of type '%s' (these should not be received). Are you sure your devices are in API mode?"
                            % (data[0], cmd_name))
            
            raise KeyError(
                "Unrecognized response packet with id byte {0}".format(data[0]))
        
        # Current byte index in the data stream
        index = 1
        
        # Result info
        info = {'id':packet['name']}
        packet_spec = packet['structure']
        
        # Parse the packet in the order specified
        for field in packet_spec:
            if field['len'] == 'null_terminated':
                field_data = b''
                
                while data[index:index+1] != b'\x00':
                    field_data += data[index:index+1]
                    index += 1
                
                index += 1
                info[field['name']] = field_data
            elif field['len'] is not None:
                # Store the number of bytes specified

                # Are we trying to read beyond the last data element?
                if index + field['len'] > len(data):
                    raise ValueError(
                        "Response packet was shorter than expected")
                
                field_data = data[index:index + field['len']]
                info[field['name']] = field_data
                               
                index += field['len']
            # If the data field has no length specified, store any
            #  leftover bytes and quit
            else:
                field_data = data[index:]
                
                # Were there any remaining bytes?
                if field_data:
                    # If so, store them
                    info[field['name']] = field_data
                    index += len(field_data)
                break
            
        # If there are more bytes than expected, raise an exception
        if index < len(data):
            raise ValueError(
                "Response packet was longer than expected; expected: %d, got: %d bytes" % (index, 
                                                                                           len(data)))
                                                                                           
        # Apply parsing rules if any exist
        if 'parsing' in packet:
            for parse_rule in packet['parsing']:
                # Only apply a rule if it is relevant (raw data is available)
                if parse_rule[0] in info:
                    # Apply the parse function to the indicated field and 
                    # replace the raw data with the result
                    info[parse_rule[0]] = parse_rule[1](self, info)                                                                                   
            
        return info
        
    def _parse_samples_header(self, io_bytes):
        header_size = 3
        
        # number of samples (always 1?) is the first byte
        sample_count = byteToInt(io_bytes[0])
        
        # part of byte 1 and byte 2 are the DIO mask ( 9 bits )
        dio_mask = (byteToInt(io_bytes[1]) << 8 | byteToInt(io_bytes[2])) & 0x01FF
        
        # upper 7 bits of byte 1 is the AIO mask
        aio_mask = (byteToInt(io_bytes[1]) & 0xFE) >> 1
        
        # sorted lists of enabled channels; value is position of bit in mask
        dio_chans = []
        aio_chans = []
        
        for i in range(0,9):
            if dio_mask & (1 << i):
                dio_chans.append(i)
        
        dio_chans.sort()
        
        for i in range(0,7):
            if aio_mask & (1 << i):
                aio_chans.append(i)
        
        aio_chans.sort()
            
        return (sample_count, dio_chans, aio_chans, dio_mask, header_size)
        
    def _parse_samples(self, io_bytes):
        sample_count, dio_chans, aio_chans, dio_mask, header_size = \
            self._parse_samples_header(io_bytes)
        
        samples = []
        
        # split the sample data into a list, so it can be pop()'d
        sample_bytes = [byteToInt(c) for c in io_bytes[header_size:]]
        
        # repeat for every sample provided
        for sample_ind in range(0, sample_count):
            tmp_samples = {}
            
            if dio_chans:
                # we have digital data
                digital_data_set = (sample_bytes.pop(0) << 8 | sample_bytes.pop(0))
                digital_values = dio_mask & digital_data_set
                
                for i in dio_chans:
                    tmp_samples['dio-{0}'.format(i)] = True if (digital_values >> i) & 1 else False
                        
            for i in aio_chans:
                analog_sample = (sample_bytes.pop(0) << 8 | sample_bytes.pop(0))
                tmp_samples['adc-{0}'.format(i)] = analog_sample
            
            samples.append(tmp_samples)
        
        return samples
        
    def send(self, cmd, **kwargs):
        # Pass through the keyword arguments
        res = self._write(self._build_command(cmd, **kwargs))
        return res
       
        
    def wait_read_frame(self):       
        frame = self._wait_for_frame()
        return self._split_response(frame.data)
        
    def __getattr__(self, name):
        if name == 'api_commands':
            raise NotImplementedError("API command specifications could not be found; use a derived class which defines 'api_commands'.")
        
        # Is shorthand enabled, and is the called name a command?
        if self.shorthand and name in self.api_commands:
            # If so, simply return a function which passes its arguments
            # to an appropriate send() call
            return lambda **kwargs: self.send(name, **kwargs)
        else:
            raise AttributeError("XBee has no attribute '%s'" % name)
