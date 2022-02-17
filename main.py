from record import *
from data_area import *
from buffer import *
import database as db

database = db.database("main_file.dat", "overflow_area.dat")


for i in range(1, 10, 1):
    database.add_record(record(5*i, 16*i+1, 16*i+2, 16*i+3))

database.add_record(record(7,7,7,7))
database.add_record(record(8,8,8,8))

database.add_record(record(17,6,8,5))
database.add_record(record(18,8,9,8))

database.add_record(record(6,6,8,8))

database.delete_record(5)
database.delete_record(18)
database.update_record(15, record(15,1,1,1))
database.update_record(7, record(8,8,8,8))

print(str(database.read_record(5)))
print(str(database.read_record(7)))
print(str(database.read_record(0)))
print(str(database.read_record(10)))
print(str(database.read_record(18)))






