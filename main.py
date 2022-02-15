from record import *
from data_area import *
from buffer import *

main_area = data_area("main_file.dat")


for i in range(10):
    main_area.write_record(i, record(i, 16*i+1, 16*i+2, 16*i+3, i+1))

main_area.write_record(0,record(0,0,0,0))
main_area.write_record(7,record(7,7,7,7,7))
main_area.write_record(11, record(10,10,10,10))

test_record = main_area.read_record(0)
index = 1
while(test_record != None):
    print(str(test_record))
    test_record = main_area.read_record(index)
    index += 1

# main_file.write(bytes(record(0,3,4,5,0)))
# main_file.write(bytes(record(2,23,24,25,2)))
# main_file.write(bytes(record(7,73,74,75)))
# main_file.flush()

# record1 = read_record(main_file, 0)
# index = 1
# while(record1 != None):
#     print(str(record1))
#     record1 = read_record_from_overflow(main_file, index)
#     index += 1

# read_page_to_buffer(main_file, main_buffer, 0)
# main_buffer.clear()
# main_buffer.append(record(6,61,62,63))
# main_buffer.append(record(8,81,82,83,3))
# write_page_to_file(main_file, main_buffer)
pass
