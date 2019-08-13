# builds dataframe with data from bulk (from ted europe) from single rows (which
# are obtained from each file)

from Tender import *
import pandas as pd
import glob, os
import pyarrow
from datetime import datetime
import numpy as np

class DfBuilder:

    def __init__(self, columns_of_interest, stride=1, dir = os.path.join("home","janislaus","ti_bulk","data"),
                save_dir = os.path.join("home","janislaus","ti_bulk","parquet")):

        self.dir =  dir
        self.file_arr = self.__get_files()
        self.stride = stride
        self.columns_of_interest = columns_of_interest
        self.save_dir = save_dir
        self.save_name = ("{}_df_stride{}.parquet.gzip"
                    .format(datetime.now().strftime("%d%m%Y_%H%M"), stride))
        #self.df = self.__build_df()

    def __get_files(self):

        return [f for f in os.listdir(self.dir, recursive = True) if isFile(os.path.join(self.dir, f))]

#        file_arr = []
#        # store all links to the different files into list
#        print(os.path.join(self.dir, '**'))
#        for filename in glob.iglob(os.path.join(self.dir, '**'), recursive=True):
#            if os.path.isfile(filename): # filter dirs
#                file_arr.append(filename)
#
#        return file_arr


    def __build_df(self):
        '''
        builds up dataframe gradually by concatenating row by row. Each tender
        file gets flattened into one row
        '''

        sparse_arr = self.file_arr[0::self.stride]
        # initialize Dataframe which will be build up gradually
        df = pd.DataFrame()

        # build up Dataframe gradually - row by row
        i = 0
        list_of_dicts = []
        parser = XmlParser(columns_of_interest=self.columns_of_interest)
        for file in sparse_arr:

            if (i%100 == 0 and i!=0):
                print("{}/{} lines added".format(i,len(sparse_arr)))
            i +=1

            new_tender = (parser.parse_data(file)
                                .maj_vote(maj_vote_list)
                                .sum_list(sum_list))
            list_of_dicts.append(new_tender.data)
        return pd.DataFrame(list_of_dicts, columns = self.columns_of_interest)

    def save_df_as_parquet(self, df, save_dir, save_name):
        df.to_parquet(os.path.join(save_dir, save_name), compression="snappy")
        return print("Dataframe successfully written to parquet file.")

    def batch_build_and_write(self, batch_size):
        sparse_arr = self.file_arr[0::self.stride]
        print(len(sparse_arr))
        num_batches = len(sparse_arr)/batch_size
        print(num_batches)
        batch_arr = np.array_split(sparse_arr, num_batches)
        list_of_dicts = []
        parser = XmlParser(columns_of_interest=self.columns_of_interest)
        folder_name = ("{}_df_stride{}_batch{}"
                        .format(datetime.now().strftime("%d%m%Y_%H%M"), stride, batch_size))
        save_dir = os.path.join(self.save_dir, folder_name)
        os.mkdir(save_dir)

        for idx, arr in enumerate(list(batch_arr)):
            for file in arr:
                new_tender = (parser.parse_data(file)
                                    .maj_vote(maj_vote_list)
                                    .sum_list(sum_list))
                list_of_dicts.append(new_tender.data)
            df = pd.DataFrame(list_of_dicts, columns = self.columns_of_interest)

            save_dir = os.path.join(self.save_dir, "save_name")
            self.save_df_as_parquet(df=df, save_dir=save_dir,
                               save_name="batch{}.parquet.snappy".format(idx))
            print("{}/{} batch/es processed and written...".format(idx+1, len(batch_arr)))
        return print("Dataframe batches successfully written to parquet files.")




if __name__=="__main__":

    maj_vote_list = ["CPV_CODE_CODE", "CA_CE_NUTS", "CA_TYPE_OTHER",
        "CA_TYPE_VALUE", "CONTACT_POINT", "IA_URL_ETENDERING", "URL",
        "IA_URL_GENERAL", "URL_BUYER", "URL_GENERAL", "MA_MAIN_ACTIVITIES",
        "MA_MAIN_ACTIVITIES_CODE", "NOTICE_NUMBER_OJ", "NO_DOC_OJS", "OFFICIALNAME",
        "TI_DOC", "COUNTRY_VALUE", "LANGUAGE_VALUE", "NATIONALID", "CRITERIA",
        "DATE", "DATE_CONCLUSION_CONTRACT", "DATE_DISPATCH_NOTICE", "DATE_END",
        "DATE_START", "DATE_PUBLICATION_NOTICE", "DELETION_DATE", "MONTH", "E_MAIL",
        "FAX", "MAIN_SITE", "NB_TENDERS_RECEIVED", "NO_OJ", "OPTIONS_DESCR",
        "POSTAL_CODE", "RECEPTION_ID", "SHORT_DESCR", "TED_EXPORT_DOC_ID", "TITLE",
        "TOWN", "TYPE_CONTRACT_CTYPE", "VAL_CURRENCY",
        "ORIGINAL_CPV", "ORIGINAL_CPV_CODE", "NUTS_CODE",
        "TENDERER_NUTS", "ORIGINAL_NUTS", "TENDERER_NUTS_CODE",
        "ORIGINAL_NUTS_CODE","DURATION", "DURATION_TYPE", "AWARD_CONTRACT", "AWARDED_CONTRACT",
        "AWARD_CONTRACT_ITEM", "AC_AWARD_CRIT", "AC_AWARD_CRIT_CODE", "NO_AWARDED_TO_GROUP",
        "CONTRACT_AWARD_DEFENCE_LG", "CONTRACT_AWARD_DEFENCE_FORM",
        "CONTRACT_AWARD_DEFENCE_VERSION", "CONTRACT_AWARD_LG",
        "CONTRACT_AWARD_FORM", "CONTRACT_AWARD_VERSION",
        "CONTRACT_AWARD_UTILITIES_CATEGORY", "CONTRACT_AWARD_UTILITIES_VERSION",
        "FD_CONTRACT_AWARD_DEFENCE_CTYPE",
        "CONTRACT_AWARD_UTILITIES_FORM", "CONTRACT_AWARD_UTILITIES_LG",
        "CONTRACT_AWARD_CATEGORY", "ISO_COUNTRY", "ISO_COUNTRY_VALUE"]

    sum_list = ["VALUE", "VAL_TOTAL", "VAL_ESTIMATED_TOTAL", "VALUE_COST",
        "VALUE_COST_FMTVAL"]


#    (DfBuilder(stride=2000, columns_of_interest=maj_vote_list+sum_list).save_df_as_parquet())
(DfBuilder(stride=1000, columns_of_interest=maj_vote_list+sum_list).batch_build_and_write(500))
