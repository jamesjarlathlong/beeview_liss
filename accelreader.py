import ustruct as struct
def get_signed_number(number):
    """given an integer representation of a C uint_16_t, convert it to the 
    signed representation"""
    bits = 16
    mask = (2**bits) - 1
    if number & (1 << (bits-1)):
        return number | ~mask
    else:
        return number & mask

def int_to_gs(number):
     """convert the signed integer representation of acceleration to gs"""
     vref = 2.4
     max_value = (2**16)/2 -1 #16 bit representation
     number_to_volt = vref/max_value
     volt_to_g = 5/3.3
     number_to_g = number_to_volt*number*volt_to_g
     return number_to_g
  
def bytestring_toaccelbytes(packet):
    """takes 240 byte string and returns 120 2byte accel values
       i.e. b'abcdef'-->['ab','cd','ef']"""
    accel_values = [packet[i:i+2] for i in range(0, len(packet), 2)]
    return accel_values

def accelbyte_toint(accelbytes):
    """takes a raw 2 byte accel value and returns the integer representation"""
    accel_int = struct.unpack('>h', accelbytes)[0]
    return accel_int  

def accelbytes_toints(accelbytes):
    """takes list of 120 2 byte accel values and returns integer representation"""
    accel_ints = map(accelbyte_toint, accelbytes)
    return list(accel_ints)

def bytestring_toaccelints(bytestring):
    """takes 240 byte string and returns 120 int list of accel values"""
    accelbytes = bytestring_toaccelbytes(bytestring)
    accel_ints = accelbytes_toints(accelbytes)
    return accel_ints

def packet_to_3accels(packet):
    """takes a packet of 240 bytes of 2byte accel values in x,y,z,x,y,z
    order and returns a tuple of three lists of accel integer values"""
    accelints = bytestring_toaccelints(packet)
    #triaccel = [accelints[i::3] for i in range(3)]
    return accelints
