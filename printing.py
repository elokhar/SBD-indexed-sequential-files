from block_rw import read_record

def print_file(file, show_overflow_indices=False):
    record = read_record(file)
    if(show_overflow_indices==False):
        while(record != None):
            print(record)  
            record = read_record(file)
    else:
        while(record != None):
            print(str(record)+" "+str(record.get_ov()))
            record = read_record(file)