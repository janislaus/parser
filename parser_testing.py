
from parser import *
import pandas as pd
import glob, os

# directory of where data of bulk is stored
dir = "/home/janislaus/ted_bulk/data"


# store all links to the different xml files into list
xml_arr = []
for filename in glob.iglob(dir + '/**', recursive=True):
    if os.path.isfile(filename): # filter dirs
        xml_arr.append(filename)

xml_arr = xml_arr[0::100]

#--------------------------------------------------------------------

# some testing on how parser is working

#a = Tender("test.xml")
#a.get_data()
#set_a = set([item[0] for item in a.data])
#
#union = len(set_a)
#print("length of set_a is: " + str(union))
#union_agg = 0
#i = 0
#xml_arr = xml_arr[0::1000]
#print("len from xml_arr: " + str(len(xml_arr)))
#for idx, file in enumerate(xml_arr):
#    parsed_file = Tender(file)
#    parsed_file.get_data()
#    b = [item[0] for item in parsed_file.data]
#    set_b = set(b)
#    print("lenb: " + str(len(b)))
#    print("lensetb: " + str(len(set_b)))
#    print(file)
#
#    union = len(set_a & set_b)
#    union_agg += union

#    if (len(set_a & set_b) < union):
#        print("new min: " + str(union))
#
#    if(idx%50 == 0 and idx != 0):
#        i += 1
#        print("union at {}: ".format(str(50*i)) + str(union))

# print("average union is: " + str(union_agg/len(xml_arr)))

#AVERAGE_UNION: 65.8 (abgeglichen mit einem xml file wo set(xml) = 90)

#---------------------------------------------------------------------

##ar = [item[0] for item in b.data]
import collections
print([(item,count) for item, count in collections.Counter(arr).items() if count > 1])
