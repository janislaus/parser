# builds dataframe with data from bulk (from ted europe) from single rows (which
# are obtained from each xml file)

from parser import *
import pandas as pd
import glob, os
import pyarrow

# directory of where data of bulk is stored
dir = "/home/janislaus/ted_bulk/data"


# store all links to the different xml files into list
xml_arr = []
for filename in glob.iglob(dir + '/**', recursive=True):
    if os.path.isfile(filename): # filter dirs
        xml_arr.append(filename)

xml_arr = xml_arr[0::1000]

# initialize Dataframe which will be build up gradually
df = pd.DataFrame()

# build up Dataframe row for row
i = 0
l = []
for xml in xml_arr:

    if (i%100 == 0 and i!=0):
        print("{} lines added".format(i))
    i +=1
    new_tender = Tender(xml)
    new_tender.get_data()

    ndict = {item[0]: [item[1]] for item in new_tender.data}
    df2 = pd.DataFrame(ndict)
    df = pd.concat([df, df2], axis=0, ignore_index=True)


# save dataframe as parquet file
df.to_parquet('parquet/df_every_1000th_file_p1.parquet.gzip', compression='gzip')
