# builds dataframe with data from bulk (from ted europe) from single rows (which
# are obtained from each file)

from Tender import *
import pandas as pd
import glob, os
import pyarrow
from datetime import datetime

class DfBuilder:

    def __init__(self, dir = "/home/janislaus/ted_bulk/data",
                save_dir = '/home/janislaus/ted_bulk/parquet/',
                stride=1):

        self.dir =  dir
        self.file_arr = self.__get_files()
        self.stride = stride
        self.save_dir = save_dir + "{}_df_stride{}.parquet.gzip".format(
                        datetime.now().strftime("%d%m%Y_%H%M"), stride)
        self.df = self.__build_df()

    def __get_files(self):

        file_arr = []
        # store all links to the different files into list
        for filename in glob.iglob(self.dir + '/**', recursive=True):
            if os.path.isfile(filename): # filter dirs
                file_arr.append(filename)

        return file_arr


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
        l = []
        parser = XmlParser(columns_of_interest=maj_vote_list+sum_list)
        for file in sparse_arr:

            if (i%100 == 0 and i!=0):
                print("{}/{} lines added".format(i,len(sparse_arr)))
            i +=1

            new_tender = parser.parse_data(file)
                                .maj_vote(maj_vote_list)
                                .sum_list(sum_list))
            df2 = pd.DataFrame(new_tender.data)

            df = pd.concat([df, df2], axis=0, ignore_index=True)

        return df

    def save_df_as_parquet(self):
        self.df.to_parquet(self.save_dir, compression="gzip")
        return print("Dataframe successfully written to parquet file.")


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
        "ORIGINAL_CPV", "ORIGINAL_CPV_CODE", "ORIGINAL_CPV_CODE", "NUTS_CODE",
        "TENDERER_NUTS", "ORIGINAL_NUTS", "TENDERER_NUTS_CODE",
        "ORIGINAL_NUTS_CODE","DURATION", "DURATION_TYPE", "AWARD_CONTRACT", "AWARDED_CONTRACT",
        "AWARD_CONTRACT_ITEM", "AC_AWARD_CRIT", "AC_AWARD_CRIT_CODE", "NO_AWARDED_TO_GROUP",
        "CONTRACT_AWARD_DEFENCE_LG", "CONTRACT_AWARD_DEFENCE_FORM",
        "CONTRACT_AWARD_DEFENCE_VERSION", "CONTRACT_AWARD_LG",
        "CONTRACT_AWARD_FORM", "CONTRACT_AWARD_VERSION",
        "CONTRACT_AWARD_UTILITIES_CATEGORY", "CONTRACT_AWARD_UTILITIES_VERSION",
        "FD_CONTRACT_AWARD_DEFENCE_CTYPE", "CONTRACT_AWARD_UTILITIES_FORM",
        "CONTRACT_AWARD_UTILITIES_FORM", "CONTRACT_AWARD_UTILITIES_LG",
        "CONTRACT_AWARD_CATEGORY", "ISO_COUNTRY", "ISO_COUNTRY_VALUE"]

    sum_list = ["VALUE", "VAL_TOTAL", "VAL_ESTIMATED_TOTAL", "VALUE_COST",
        "VALUE_COST_FMTVAL"]


    (DfBuilder(stride=500).save_df_as_parquet())
