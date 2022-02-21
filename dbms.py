import database as db

curr_db = db.database("main_file.dat", "overflow_area.dat")


def add_record(record):
    curr_db.add_record(record)
    curr_db.print_rw()
    if curr_db.check_reorg():
        reorganize()

def read_record(key):
    result = curr_db.read_record(key)
    curr_db.print_rw()
    return result

def update_record(key, record):
    curr_db.update_record(key, record)
    curr_db.print_rw()

def delete_record(key):
    curr_db.delete_record(key)
    curr_db.print_rw()

def reorganize():
    print("Reorganizing database")
    global curr_db
    old_db = curr_db
    old_db.flush()
    #old_db.print_database()
    new_db = db.database("new_main_file.dat", "new_overflow_file.dat")
    index_in_main = 0
    location = {"area": "main", "index": index_in_main}
    record = old_db.read_record_from_location(location)
    while record != None:
        if location["area"] == "main":
            if record.has_ov():
                location["area"] = "overflow"
                location["index"] = record.get_ov()
            else:
                index_in_main += 1
                location["index"] = index_in_main
        elif location["area"] == "overflow":
            if record.has_ov():
                location["index"] = record.get_ov()
            else:
                location["area"] = "main"
                index_in_main += 1
                location["index"] = index_in_main
        else:
            assert False, "Wrong area name in location"
        if record.check_deleted() == False:
            record.clear_ov()
            new_db.add_record(record)
        record = old_db.read_record_from_location(location)  
    old_db.print_rw()
    old_db.close_data_files()
    del old_db
    new_db.flush()
    new_db.rename_data_files("main_file.dat", "overflow_area.dat")
    new_db.set_ov_size()
    curr_db = new_db
    curr_db.print_rw()
    #curr_db.print_database()
    
def print_db():
    curr_db.print_database()

def close_current_db():
    curr_db.close_data_files()
    


