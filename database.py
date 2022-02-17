import data_area as da
import record as r

class database:
        
    def __init__(self, main_file_name, overflow_file_name):
        self.main_area = da.data_area(main_file_name)
        self.overflow_area = da.data_area(overflow_file_name)
        self.index = []
        self.overflow_length = 0
        self.max_overflow_length = da.BLOCKING_FACTOR

    def add_record(self, new_record):
        curr_record = self.main_area.read_record(0)
        next_record = self.main_area.read_record(1)
        curr_index = 0
        if curr_record == None:
                self.main_area.write_record(curr_index, new_record)
                self.update_index_file(0, new_record.get_key())
        elif new_record.get_key() < curr_record.get_key():
            #reorganise
            pass
        else:      
            while True:
                if next_record == None:
                    writing_index = curr_index + 1
                    self.main_area.write_record(writing_index, new_record)
                    self.update_index_file(writing_index, new_record.get_key())
                    break
                elif new_record.get_key() < next_record.get_key():
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
        if curr_record.get_ov() == r.NULL_INDEX:
            self.overflow_area.write_record(self.overflow_length, new_record)
            curr_record.set_ov(self.overflow_length)
            self.main_area.write_record(curr_index, curr_record)
            self.overflow_length += 1
        else:
            while curr_record.get_ov() != r.NULL_INDEX:
                curr_index = curr_record.get_ov()
                curr_record = self.overflow_area.read_record(curr_index)
            self.overflow_area.write_record(self.overflow_length, new_record)
            curr_record.set_ov(self.overflow_length)
            self.overflow_area.write_record(curr_index, curr_record)
            self.overflow_length += 1

    def read_record(self, key):
        record_location = self.find_record(key)
        if record_location == None:
            result = None
        else:
            area = record_location["area"]
            result = area.read_record(record_location["index"])
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
                    result = {"area": self.main_area, "index" : curr_index}
                    break
                elif next_record == None or next_record.get_key() > key:
                    while curr_record.get_ov() != r.NULL_INDEX:
                        curr_ov_index = curr_record.get_ov()
                        curr_record = self.overflow_area.read_record(curr_ov_index)
                        if curr_record.get_key() == key:
                            result = {"area": self.overflow_area, "index" : curr_ov_index}
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
            area = record_location["area"]
            index = record_location["index"]
            record = area.read_record(index)
            record.mark_deleted()
            area.write_record(index, record)

    def update_record(self, key, new_record):
        if new_record.get_key() == key:
            location = self.find_record(key)
            old_record = location["area"].read_record(location["index"])
            new_record.set_ov(old_record.get_ov())
            location["area"].write_record(location["index"], new_record)
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


