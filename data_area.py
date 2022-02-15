import record as r
import buffer as b

BLOCKING_FACTOR = 4
PAGE_SIZE = r.RECORD_SIZE * BLOCKING_FACTOR

class data_area():
    
    datafile = None
    buffer = None

    def __init__(self, datafile_name):
        self.datafile = open(datafile_name, "w+b")
        self.buffer = b.buffer()

    def __del__(self):
        self.datafile.close()


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
            if(self.buffer.get_page_number() != -1):
                self.write_page_to_file()
            self.read_page_to_buffer(needed_page_number)
        



    def read_page_to_buffer(self, page_number):
        buffer = self.buffer
        file = self.datafile
        buffer.clear()
        file.seek(page_number * PAGE_SIZE, 0)
        for _ in range(BLOCKING_FACTOR):
            record_bytes = file.read(r.RECORD_SIZE)
            if(record_bytes == b''):
                break               #stop reading if end of file
            buffer.append(r.record.from_bytes(record_bytes))
        buffer.set_page_number(page_number)


    def write_page_to_file(self):
        file = self.datafile
        buffer = self.buffer
        file.seek(buffer.get_page_number() * PAGE_SIZE, 0)
        for record in buffer:
            file.write(bytes(record))
        file.flush()
