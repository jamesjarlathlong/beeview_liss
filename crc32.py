import ustruct as struct
import sys

# Polynoms in reversed notation
POLYNOMS = {
  'CRC-32-IEEE': 0xedb88320, # 802.3
  'CRC-32C': 0x82F63B78, # Castagnoli
  'CRC-32K': 0xEB31D82E, # Koopman
  'CRC-32Q': 0xD5828281,
}

class Error(Exception):
  pass

class CRC32(object):
  """A class to calculate and manipulate CRC32.
  
  Use one instance per type of polynom you want to use.
  Use calc() to calculate a crc32.
  Use forge() to forge crc32 by adding 4 bytes anywhere.
  """
  def __init__(self, type="CRC-32-IEEE"):
    if type not in POLYNOMS:
      raise Error("Unknown polynom. %s" % type)
    self.polynom = POLYNOMS[type]
    self.table, self.reverse = [0]*256, [0]*256
    self._build_tables()

  def _build_tables(self):
    for i in range(256):
      fwd = i
      rev = i << 24
      for j in range(8, 0, -1):
        # build normal table
        if (fwd & 1) == 1:
          fwd = (fwd >> 1) ^ self.polynom
        else:
          fwd >>= 1
        self.table[i] = fwd & 0xffffffff
        # build reverse table =)
        if rev & 0x80000000 == 0x80000000:
          rev = ((rev ^ self.polynom) << 1) | 1
        else:
          rev <<= 1
        rev &= 0xffffffff
        self.reverse[i] = rev

  def calc(self, s):
    """Calculate crc32 of a string.
    Same crc32 as in (binascii.crc32)&0xffffffff.
    """
    crc = 0xffffffff
    for c in s:
      crc = (crc >> 8) ^ self.table[(crc ^ c) & 0xff]
    return crc^0xffffffff
      
  def scan_and_check(self, buff, packet_size):
      current_position = 0
      while current_position+packet_size<len(buff):
          end_position = packet_size+current_position
          check_block = buff[current_position:end_position]
          check_sum = struct.unpack(">L", buff[end_position:end_position+4])[0]
          if self.calc(check_block) == check_sum: #if the crc matches
              #print('currently: ', current_position)
              return(check_block[0:240], current_position)
          else:
              current_position+=1
              #print('currently: ', current_position)
      raise ValueError('No valid packet was found in the buffer')
  
  def check_packet(self, packet):
      check_block = packet[0:250]
      check_sum = struct.unpack(">L", packet[250:254])[0]
      if self.calc(check_block) == check_sum: #if the crc matches
          return check_block[0:240]
      else:
          raise ValueError('Not a valid packet')        
  
  def forge(self, wanted_crc, s, pos=None):
    """Forge crc32 of a string by adding 4 bytes at position pos."""
    if pos is None:
      pos = len(s)
    
    # forward calculation of CRC up to pos, sets current forward CRC state
    fwd_crc = 0xffffffff
    for c in s[:pos]:
      fwd_crc = (fwd_crc >> 8) ^ self.table[(fwd_crc ^ ord(c)) & 0xff]
    
    # backward calculation of CRC up to pos, sets wanted backward CRC state
    bkd_crc = wanted_crc^0xffffffff
    for c in s[pos:][::-1]:
      bkd_crc = ((bkd_crc << 8)&0xffffffff) ^ self.reverse[bkd_crc >> 24] ^ ord(c)
    
    # deduce the 4 bytes we need to insert
    for c in struct.pack('<L',fwd_crc)[::-1]:
      bkd_crc = ((bkd_crc << 8)&0xffffffff) ^ self.reverse[bkd_crc >> 24] ^ ord(c)
    
    res = s[:pos] + struct.pack('<L', bkd_crc) + s[pos:]
    assert(crc32(res) == wanted_crc)
    return res

if __name__=='__main__':
    pass
    #signed_acc = [list(map(crc.get_signed_number, i)) for i in acc]
    #g_acc = [list(map(crc.int_to_gs, j)) for j in signed_acc]
    #print('in gs: ', acc)
    
