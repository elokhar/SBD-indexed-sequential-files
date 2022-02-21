import dbms
from sys import stdin
import record as r

def run_interface(input_file=None):
    command=""
    if(input_file==None):
        input_stream = stdin
    else:
        input_stream = input_file
    while command!="quit":
        command = input_stream.readline().strip()
        command = command.split(" ")
        db_modified = False
        keyword = command[0]
        arguments = command[1:]
        for i in range(len(arguments)):
            arguments[i] = int(arguments[i])
        match keyword:
            case "add":
                if len(arguments) < 4:
                    print("Too few arguments, cannot add record")
                else:
                    dbms.add_record(r.record(arguments[0], arguments[1], arguments[2], arguments[3]))
                    db_modified = True
            case "read":
                if len(arguments) < 1:
                    print("Too few arguments, cannot read record")
                else:
                    print(dbms.read_record(arguments[0]))
            case "update":
                if len(arguments) < 5:
                    print("Too few arguments, cannot update record")
                else:
                    dbms.update_record(arguments[0], r.record(arguments[1], arguments[2], arguments[3], arguments[4]))
                    db_modified = True    
            case "delete":
                if len(arguments) < 1:
                    print("Too few arguments, cannot delete record")
                else:
                    dbms.delete_record(arguments[0])
                    db_modified = True
            case "reorganize":
                dbms.reorganize()    
                db_modified = True

        if db_modified:
            dbms.print_db()
    
    dbms.close_current_db()
            