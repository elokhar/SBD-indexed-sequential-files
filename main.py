import interface as i
import data_areas as da

with open("main_file.dat","w+b") as main_file,\
open("index_file.dat","w+b") as index_file,\
open("overflow_area.dat","w+b") as ov_file:
    ov_area = da.ov_area(ov_file)
    i.run_interface(main_file, index_file, ov_area)




