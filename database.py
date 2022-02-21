import data_area as da
import record as r

class database:

    show_rw = False
        
    def __init__(self, main_file_name, overflow_file_name):
        self.main_area = da.data_area(main_file_name)
        self.overflow_area = da.data_area(overflow_file_name)
        self.index = []
        self.main_length = 0
        self.overflow_length = 0
        self.max_overflow_length = da.BLOCKING_FACTOR
        self.total_reads = 0
        self.total_writes = 0
        guardian_record = r.record(0,0,0,0)
        guardian_record.mark_deleted()
        self.add_record(guardian_record)


    def add_record(self, new_record):
        if not self.index:
            curr_index = 0
        else:
            page_number = 0
            while page_number < len(self.index):
                if self.index[page_number] > new_record.get_key():
                    break
                page_number += 1
            curr_index = (page_number-1) * da.BLOCKING_FACTOR    
        curr_record = self.main_area.read_record(curr_index)
        next_record = self.main_area.read_record(curr_index + 1)
        
        if curr_record == None:
                self.main_area.write_record(curr_index, new_record)
                self.main_length += 1
                self.update_index_file(0, new_record.get_key())             
        else:      
            for i in range(da.BLOCKING_FACTOR):
                if next_record == None:
                    writing_index = curr_index + 1
                    self.main_area.write_record(writing_index, new_record)
                    self.main_length += 1
                    self.update_index_file(writing_index, new_record.get_key())
                    break
                elif new_record.get_key() < next_record.get_key() or i == da.BLOCKING_FACTOR - 1:
                    self.add_to_overflow(new_record, curr_index)
                    break
                else:
                    curr_record = next_record
                    curr_index += 1
                    next_record = self.main_area.read_record(curr_index + 1)

    def add_to_overflow(self, new_record, curr_index):
        if self.overflow_length == self.max_overflow_length:
            #reorganise
            pass
        curr_record = self.main_area.read_record(curr_index)
        next_record = self.overflow_area.read_record(curr_record.get_ov())
        if next_record == None or next_record.get_key() > new_record.get_key():
            if next_record != None:
                new_record.set_ov(curr_record.get_ov())
            curr_record.set_ov(self.overflow_length)
            self.main_area.write_record(curr_index, curr_record)
            self.overflow_area.write_record(self.overflow_length, new_record)

            self.overflow_length += 1
        else:
            next_record = self.overflow_area.read_record(curr_record.get_ov())
            while next_record != None:
                
                if next_record.get_key() > new_record.get_key():
                    break
                curr_index = curr_record.get_ov()
                curr_record = next_record            
                next_record = self.overflow_area.read_record(curr_record.get_ov())
                # curr_index = curr_record.get_ov()
                # curr_record = self.overflow_area.read_record(curr_index)
            
            if curr_record.has_ov():
                new_record.set_ov(curr_record.get_ov())
            curr_record.set_ov(self.overflow_length)
            self.overflow_area.write_record(curr_index, curr_record)
            self.overflow_area.write_record(self.overflow_length, new_record)
            self.overflow_length += 1

    def read_record(self, key):
        record_location = self.find_record(key)
        result = self.read_record_from_location(record_location)
        return result
    
    def read_record_from_location(self, location):
        if location == None:
            result = None
        else:
            match location["area"]:
                case "main":
                    area = self.main_area
                case "overflow":
                    area = self.overflow_area
            index = location["index"]
            result = area.read_record(index)
        return result
                
    def find_record(self, key):
        result = None
        if not self.index or key < self.index[0]:
            pass
        else:
            page_number = 0
            while page_number < len(self.index):
                if self.index[page_number] > key:
                    break
                page_number += 1
            curr_index = (page_number-1) * da.BLOCKING_FACTOR
            curr_record = self.main_area.read_record(curr_index)
            next_record = self.main_area.read_record(curr_index + 1)
            
            while True:
                if curr_record == None:
                    break
                elif curr_record.get_key() == key:
                    result = {"area": "main", "index" : curr_index}
                    break
                elif next_record == None or next_record.get_key() > key:
                    while curr_record.get_ov() != r.NULL_INDEX:
                        curr_ov_index = curr_record.get_ov()
                        curr_record = self.overflow_area.read_record(curr_ov_index)
                        if curr_record.get_key() == key:
                            result = {"area": "overflow", "index" : curr_ov_index}
                            break
                    break
                else:
                    curr_record = next_record
                    curr_index += 1
                    next_record = self.main_area.read_record(curr_index + 1)
            return result

    def delete_record(self, key):
        record_location = self.find_record(key)
        if record_location == None:
            pass
        else:
            area = self.match_area(record_location["area"])
            index = record_location["index"]
            record = area.read_record(index)
            record.mark_deleted()
            area.write_record(index, record)

    def update_record(self, key, new_record):
        if new_record.get_key() == key:
            location = self.find_record(key)
            area = self.match_area(location["area"])
            old_record = area.read_record(location["index"])
            new_record.set_ov(old_record.get_ov())
            area.write_record(location["index"], new_record)
        else:
            self.delete_record(key)
            self.add_record(new_record)

    def update_index_file(self, record_index, record_key):
        if record_index % da.BLOCKING_FACTOR != 0:
            pass
        else:
            page_number = record_index // da.BLOCKING_FACTOR
            if page_number > len(self.index):
                assert False, "Index cannot be updated, such page does not exist"
            elif page_number == len(self.index):
                self.index.append(record_key)
            else:
                self.index[page_number] = record_key

    def match_area(self, area_str):
        area = None
        match area_str:
            case "main":
                area = self.main_area
            case "overflow":
                area = self.overflow_area
        return area

    def flush(self):
        self.main_area.write_page_to_file()
        self.overflow_area.write_page_to_file()

    def close_data_files(self):
        self.main_area.close_file()
        self.overflow_area.close_file()

    def rename_data_files(self, main_file_name, overflow_file_name):
        self.main_area.rename_file(main_file_name)
        self.overflow_area.rename_file(overflow_file_name)

    def set_ov_size(self):
        self.max_overflow_length = self.main_length // 3 + 1

    def check_reorg(self):
        return self.overflow_length > self.max_overflow_length

    def print_database(self):
        print("Main area:")
        self.main_area.print_area()
        print("Overflow area:")
        self.overflow_area.print_area()
        print()

    def print_rw(self):
        new_reads = self.main_area.disk_reads + self.overflow_area.disk_reads - self.total_reads
        new_writes = self.main_area.disk_writes + self.overflow_area.disk_writes - self.total_writes
        if self.show_rw:
            print("r="+str(new_reads)+" w="+str(new_writes))
        self.total_reads += new_reads
        self.total_writes += new_writes

