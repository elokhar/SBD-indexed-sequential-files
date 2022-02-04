import record as r
from block_rw import read_record, write_record, BLOCKING_FACTOR

def add_record(new_record, main_file, index_file, ov_area):
    #start_reading_from_beginning(main_file)
    page_number=0
    position = 0
    curr_record = read_record(main_file, page_number, position)
    if(curr_record == None):    #if page is empty
        write_record(main_file, page_number, position, new_record)
    elif(new_record.get_key() < curr_record.get_key()):
        pass #reorganise
    else:
        while(curr_record!=None):
            position += 1
            next_record = read_record(main_file, page_number, position)
            if(position >= BLOCKING_FACTOR-1):  #if last record on page reached
                new_record_index = ov_area.add_record_to_overflow(new_record, page_number, position)
                curr_record.set_ov(new_record_index)
                write_record(main_file, page_number, position-1, curr_record)
            elif(next_record.get_key()):
                pass
            curr_record = next_record
            

        






         