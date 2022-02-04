from block_rw import BLOCKING_FACTOR, read_record, write_record
from record import NULL_INDEX, RECORD_SIZE


class ov_area:

    ov_file = None
    max_pages_held = 1
    records_held = 0
    max_records_held = BLOCKING_FACTOR
    full = False

    def __init__(self, ov_file):
        self.ov_file = ov_file

    def change_size(self, number_of_pages):
        self.max_pages_held = number_of_pages
        self.max_records_held = number_of_pages * BLOCKING_FACTOR

    def add_record_to_overflow(self, new_record, curr_record_page, curr_record_position):
        last_page = self.records_held / BLOCKING_FACTOR
        position = self.records_held % BLOCKING_FACTOR
        write_record(self.ov_file, last_page, position, new_record)
        self.upadate_ov_index(curr_record_page, curr_record_position, self.records_held)
        self.records_held+=1
        # ov_index = curr_record.get_ov()
        # if(ov_index==NULL_INDEX):
        #     write_record(self.ov_file, new_record)
        #     new_record_index = self.records_held
        #     self.records_held += 1
        #     if(self.records_held >= self.max_records_held):
        #         self.full = True
        # else:
            # start_reading_from_beginning(self.ov_file)
            # self.ov_file.seek(RECORD_SIZE*ov_index)
            # next_record = read_record(self.ov_file)
            # self.add_record_to_overflow(new_record, next_record)
            #pass

    def update_ov_index(self, curr_page, curr_position, new_index):
        curr_record = read_record(self.ov_file, curr_page, curr_position)
        curr_index = curr_record.get_ov()
        if(curr_index==NULL_INDEX):
            curr_record.set_ov(new_index)
            write_record(self.ov_file, curr_page, curr_position, curr_record)
        else:
            next_page = self.calculate_page(curr_index)
            next_position = self.calculate_position(curr_index)
            self.update_ov_index(next_page, next_position, new_index)

    def calculate_page(index):
        return index / BLOCKING_FACTOR

    def calculate_position(index):
        return index % BLOCKING_FACTOR



