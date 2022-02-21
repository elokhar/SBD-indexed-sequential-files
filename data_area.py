import record as r
import buffer as b
from os import rename, remove
from os.path import exists

BLOCKING_FACTOR = 4
PAGE_SIZE = r.RECORD_SIZE * BLOCKING_FACTOR

class data_area:


    def __init__(self, datafile_name):
        self.datafile = open(datafile_name, "w+b")
        self.buffer = b.buffer()
        self.file_closed = False
        self.disk_reads = 0
        self.disk_writes = 0


    def read_record(self, index):  #function for reading using record index
        no_record_at_index = False
        if index < 0:
            no_record_at_index = True
        else:
            self.load_page_to_buffer(index)
            index_on_page = index % BLOCKING_FACTOR
            if(index_on_page >= len(self.buffer)):
                no_record_at_index = True
            else:
                record = self.buffer[index_on_page]
        if no_record_at_index:
            return None
        else:
            return record

    def write_record(self, index, record):
        if (index < 0): 
            assert False, "Index for writing is negative"
        else:
            self.load_page_to_buffer(index)
            index_on_page = index % BLOCKING_FACTOR
            if (index_on_page < len(self.buffer)):
                self.buffer[index_on_page] = record
            elif (index_on_page == len(self.buffer)):
                self.buffer.append(record)
            else:
                assert False, "Index for writing is too big"

    def load_page_to_buffer(self, index):
        needed_page_number = index // BLOCKING_FACTOR
        if (self.buffer.get_page_number() != needed_page_number):
            self.write_page_to_file()
            self.read_page_to_buffer(needed_page_number)
        
    def read_page_to_buffer(self, page_number):
        buffer = self.buffer
        file = self.datafile
        buffer.clear()
        buffer.is_modified = False
        file.seek(page_number * PAGE_SIZE, 0)
        page_bytes = file.read(r.RECORD_SIZE * BLOCKING_FACTOR)
        self.disk_reads += 1
        for i in range(BLOCKING_FACTOR):
            record_bytes = page_bytes[i*r.RECORD_SIZE : (i+1)*r.RECORD_SIZE]
            if(record_bytes == b''):
                break               #stop reading if end of file
            buffer.append(r.record.from_bytes(record_bytes))
        buffer.set_page_number(page_number)

    def write_page_to_file(self):
        file = self.datafile
        buffer = self.buffer
        if buffer.get_page_number() != -1 and buffer.is_modified == True:
            page_bytes = b''
            for record in buffer:
                page_bytes += bytes(record)
            file.seek(buffer.get_page_number() * PAGE_SIZE, 0)
            file.write(page_bytes)
            self.disk_writes += 1
            file.flush()
            buffer.clear()
        buffer.set_page_number(-1)

    def close_file(self):
        self.write_page_to_file()
        self.datafile.close()

    def rename_file(self, new_file_name):
        self.datafile.close()
        if exists(new_file_name):
            remove(new_file_name)
        rename(self.datafile.name, new_file_name)
        self.datafile = open(new_file_name, "r+b")

    def print_area(self):
        index = 0
        record = self.read_record(index)
        if record == None:
            print("Empty")
        else:
            while record != None:
                print(str(record))
                index += 1
                record = self.read_record(index) 
