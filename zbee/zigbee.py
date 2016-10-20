import ustruct as struct
from zbee.base import XBeeBase
from zbee.async_base import XBeeAsync
from zbee.python2to3 import byteToInt, intToByte

class ZigBee(XBeeAsync):

    api_commands = {
                    "tx":
                        [{'name':'id',              'len':1,        'default':b'\x10'},
                         {'name':'frame_id',        'len':1,        'default':b'\x01'},
                         {'name':'dest_addr_long',  'len':8,        'default':None},
                         {'name':'dest_addr',       'len':2,        'default':None},
                         {'name':'broadcast_radius','len':1,        'default':b'\x00'},
                         {'name':'options',         'len':1,        'default':b'\x00'},
                         {'name':'data',            'len':None,     'default':None}]
                    }
    
    api_responses = {b"\x90":
                        {'name':'rx',
                         'structure':
                            [{'name':'source_addr_long','len':8},
                             {'name':'source_addr',     'len':2},
                             {'name':'options',         'len':1},
                             {'name':'rf_data',         'len':None}]},
                     b"\x8b":
                        {'name':'tx_status',
                         'structure':
                            [{'name':'frame_id',        'len':1},
                             {'name':'dest_addr',       'len':2},
                             {'name':'retries',         'len':1},
                             {'name':'deliver_status',  'len':1},
                             {'name':'discover_status', 'len':1}]},
                     b"\x8a":
                        {'name':'status',
                         'structure':
                            [{'name':'status',      'len':1}]},
                     b"\x95":
                        {'name':'node_id_indicator',
                         'structure':
                            [{'name':'sender_addr_long','len':8},
                             {'name':'sender_addr',     'len':2},
                             {'name':'options',         'len':1},
                             {'name':'source_addr',     'len':2},
                             {'name':'source_addr_long','len':8},
                             {'name':'node_id',         'len':'null_terminated'},
                             {'name':'parent_source_addr','len':2},
                             {'name':'device_type',     'len':1},
                             {'name':'source_event',    'len':1},
                             {'name':'digi_profile_id', 'len':2},
                             {'name':'manufacturer_id', 'len':2}]}
                     }
    
    def __init__(self, *args, **kwargs):
        # Call the super class constructor to save the serial port
        super(ZigBee, self).__init__(*args, **kwargs)

    def _parse_samples_header(self, io_bytes):
        header_size = 4

        # number of samples (always 1?) is the first byte
        sample_count = byteToInt(io_bytes[0])
        
        # bytes 1 and 2 are the DIO mask; bits 9 and 8 aren't used
        dio_mask = (byteToInt(io_bytes[1]) << 8 | byteToInt(io_bytes[2])) & 0x0E7F
        
        # byte 3 is the AIO mask
        aio_mask = byteToInt(io_bytes[3])
        
        # sorted lists of enabled channels; value is position of bit in mask
        dio_chans = []
        aio_chans = []
        
        for i in range(0,13):
            if dio_mask & (1 << i):
                dio_chans.append(i)
        
        dio_chans.sort()
        
        for i in range(0,8):
            if aio_mask & (1 << i):
                aio_chans.append(i)
        
        aio_chans.sort()
        
        return (sample_count, dio_chans, aio_chans, dio_mask, header_size)
