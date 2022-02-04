import sys
import os
from block_rw import read_record, write_record
from data_areas import ov_area
from printing import print_file
import record as r
import dbms


def run_interface(database, index_file, ov_area, input_file=None):
    command=""
    test_file = None
    if(input_file==None):
        input_stream = sys.stdin
    else:
        input_stream = input_file
    while command!="quit":
        command = input_stream.readline().strip()
        match command:
            case "add":
                print("Type records to add in a form: \"key c m dT\", type \"end\" to end adding records:")
                record_str = input_stream.readline().strip()
                while(record_str!="end"):
                    record = record_from_string(record_str)
                    if(record!=None):
                        dbms.add_record(record,database, index_file, ov_area)
                        print("Record added")
                    else:
                        print("Cannot add record, too few valid parameters")
                    record_str=input_stream.readline().strip()
                print("Exiting adding mode")
            case "read all":
                print("Main file:")
                print_file(database)
                print("Overflow area:")
                print_file(ov_area.ov_file, True)
            case "test_file":
                print("Enter test file name with extension, leave empty to use test.txt:")
                filename = input_stream.readline().strip()
                if(filename==""):
                    filename="test.txt"
                if(os.path.isfile(filename)):
                    test_file = open(filename)
                    run_interface(database,index_file, ov_area,test_file)
                    test_file.close()
                else:
                    print("No such file exists!")
            case "quit":
                if(input_file!=None):
                    print("End of test file parsing")
                else:
                    print("Exiting")
            case _:
                print("Invalid command used")
    
            
            
def record_from_string(record_str):
    parameters = []
    curr_parameter_str = ""
    record_str+=' '
    for character in record_str:
        if(character!=' '):
            curr_parameter_str+=character
        else:
            if(curr_parameter_str.isdigit()):
                parameters.append(int(curr_parameter_str))
                if(len(parameters)>=4):
                    break
            curr_parameter_str=""
    if(len(parameters)>=4):
        return r.record(parameters[0], parameters[1], parameters[2], parameters[3])      
    else:
        return None  

            

