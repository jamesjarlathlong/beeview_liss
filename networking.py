import json as json

def json_to_bytes(dict_msg):
    return bytes(json.dumps(dict_msg,separators=(',',':')), 'ascii')

def split_string_into_list(string, n):
    return [string[x:x+n] for x in range(0, len(string), n)]

def add_chksum_info_tochunks(chunks, user, chunk_type):
    full_chunk_msgs = [{'u':user, chunk_type:chunk,'n': idx,'c':len(chunks)}
                       for idx, chunk in enumerate(chunks)]
    return full_chunk_msgs

def chunk_data_to_payload(dict_msg):
    """take a single message in json form
    {something:something, 'u':username}, where u is optional
    and split into a list of strings, each less than x bytes"""
    try:
        user = dict_msg['u']
        del dict_msg['u']
    except KeyError:
        print('no user in this message')
        user = None
    possible_chunks = ('kv','s','res','fn') #different types of messages we might want to send
    chunk_type = set(dict_msg).intersection(possible_chunks).pop()
    string_msg = json.dumps(dict_msg[chunk_type], separators=(',',':'))
    #convert {res:something} to '{res:something}', or {kv_values:(a,b)} to '{kv_values:(a,b)}'
    string_list = split_string_into_list(string_msg, 23)
    full_msgs = add_chksum_info_tochunks(string_list, user, chunk_type)
    return full_msgs
