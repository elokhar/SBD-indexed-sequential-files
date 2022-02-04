import record as r

BLOCKING_FACTOR = 4    #number of records in one block
PAGE_SIZE = r.RECORD_SIZE*BLOCKING_FACTOR     #block size in bytes
NUMBER_OF_BUFFERS = 3

#records_list = []   #the buffer for reading and writing
buffers = [ [] for _ in range(NUMBER_OF_BUFFERS) ]
buffers_modes = [ "read" for _ in range(NUMBER_OF_BUFFERS) ]
buffers_pages = [ -1 for _ in range(NUMBER_OF_BUFFERS) ]

def read_record(data_file, page_number, position):
    buffer = load_buffer(data_file, page_number)
    if position >= len(buffer):
        return None
    else:
        return buffer[position]

def write_record(data_file, page_number, position, record):    
    buffer = load_buffer(data_file, page_number)
    assert position <= len(buffer), "Index out of range?"
    if position == len(buffer):
        buffer.append([])
    buffer[position] = record

def load_buffer(data_file, needed_page_number):
    buffer_number = select_buffer_number(data_file)
    stored_page_number = buffers_pages[buffer_number] 
    #set_buffer_mode(buffer_number, "write", data_file)
    buffer = buffers[buffer_number]
    if not buffer:
        read_page_to_buffer(data_file, needed_page_number, buffer_number)
    elif(needed_page_number != stored_page_number):
        write_page_to_file(data_file, stored_page_number, buffer_number)
        read_page_to_buffer(data_file, needed_page_number, buffer_number)
    return buffer

# def read_block_to_buffer(data_file, buffer_number):
#     records_list = buffers[buffer_number]
#     file = data_file
#     record_bytes = file.read(r.RECORD_SIZE)
    
#     if(record_bytes==b''): #if there is no more data to read, the function returns reads nothing and returns False
#         return False
#     else:
#         records_list.append(r.record.from_bytes(record_bytes))
#         for i in range(r.RECORD_SIZE,PAGE_SIZE,r.RECORD_SIZE):
#             record_bytes = file.read(r.RECORD_SIZE)
#             if(record_bytes==b''): 
#                 break               #stop reading if end of file has been reached
#             records_list.append(r.record.from_bytes(record_bytes))
#         return True

def read_page_to_buffer(file, page_number, buffer_number):
    buffer = buffers[buffer_number]
    buffer = [None] * BLOCKING_FACTOR                                #????
    file.seek(page_number*PAGE_SIZE,0)
    for _ in range(0,PAGE_SIZE,r.RECORD_SIZE):
        record_bytes = file.read(r.RECORD_SIZE)
        if(record_bytes==b''): 
            break               #stop reading if end of file has been reached
        buffer.append(r.record.from_bytes(record_bytes))

def write_page_to_file(file, page_number, buffer_number):
    buffer = buffers[buffer_number]
    file.seek(page_number*PAGE_SIZE,0)
    for record in buffer:
        file.write(bytes(record))
    file.flush()
    buffer.clear()

# def write_buffer_to_file(data_file, buffer_number):
#     records_list = buffers[buffer_number]
#     file = data_file
#     for record in records_list:
#         file.write(bytes(record))
#     file.flush()
#     records_list.clear()

# def start_reading_from_beginning(file):
#     buffer_number = select_buffer_number(file)
#     set_buffer_mode(buffer_number,"read_from_beginning",file)

# def set_buffer_mode(buffer_number, mode, file):
#     # if(buffers_modes[buffer_number]==mode):
#     #     pass
#     # else:
#     #     if(mode=="read" or mode=="read_from_beginning"):   #if mode changed to "read"
#     #         write_buffer_to_file(file, buffer_number)
#     #         file.seek(0)        #move file cursor to the beginning of the file
#     #         buffers[buffer_number].clear()
#     #         buffers_modes[buffer_number] = "read"
#     #     elif(mode=="write"):  #if mode changed to "write"
#     #         file.seek(0,2)      #move file cursor to the end of the file
#     #         buffers[buffer_number].clear()
#     #         buffers_modes[buffer_number] = "write"
#     #     else:
#     #         assert False, "Incorrect buffer mode used!"
#     pass

def select_buffer_number(file):
    file_name_no_ext = file.name[:-4]
    match file_name_no_ext:
        case "main_file":
            buffer_number=0
        case "index_file":
            buffer_number=1
        case "overflow_area":
            buffer_number=2      
        case _:
            buffer_number=-1

    assert buffer_number!=-1, "Wrong filename used, cannot assign buffer!"

    return buffer_number





        
        


    

