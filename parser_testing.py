
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

##ar = [item[0] for item in b.data]
import collections
print([(item,count) for item, count in collections.Counter(arr).items() if count > 1])
