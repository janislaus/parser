# builds dataframe with data from bulk (from ted europe) from single rows (which
# are obtained from each file)

from Tender import *
import pandas as pd
import glob, os
import pyarrow
from datetime import datetime
import numpy as np


def get_files(dir_data=DIR_DATA):
    """
    Collects all files for later parsing in an array.

    :param dir_data: Directory in which to look for files
    :returns: array of each path of a file
    """
    print("Collecting files...")
    files = []
    for filename in glob.iglob(os.path.join(DIR_DATA, "**"), recursive=True):
        if os.path.isfile(filename): # filter dirs
            files.append(filename)
    return files

def save_df_as_parquet(df, dir_save, save_name):
    """
    Writes a pandas dataframe into a parquet file.

    :param df: pandas dataframe
    :param save_dir: Directory, where dataframe gets written into
    :param save_name: Name of parquet file
    """
    df.to_parquet(os.path.join(dir_save, save_name), compression="snappy")

def build_batch(columns, batch):
    """
    Builds up a batch of dictionaries into a list of dictionaries which then is
    transformed into a pandas dataframe
    :param batch: list of files to be parsed into dictionaries
    """

    list_of_dicts = []
    for idx, file in enumerate(list(batch)):
        new_tender = (parser.parse_data(file)
                            .transform(maj_vote, columns=maj_vote_list)
                            .transform(sum_entries, columns=sum_list))
        list_of_dicts.append(new_tender.data)
    return pd.DataFrame(list_of_dicts, columns = columns)

def parse_and_write(files, columns, stride=1, batch_size=500, dir_save=DIR_SAVE):
    """
    Parses a given set of files, batches them and writes batches to parquet.
    :param files List of files that are being processed
    :param batch_size: Size of batch (= number of files) that make are written
                       into one parquet file
    :returns: Success message

    """
    sparse_arr = files[0::stride]
    batch_arr = np.array_split(sparse_arr, len(sparse_arr)/batch_size)

    #print(stride)
    #print(len(batch_arr))
    #print(len(sparse_arr)/batch_size)
    #print()
    dir_save = os.path.join(dir_save, "{}_df_stride{}_batch{}".format(datetime.now()
                    .strftime("%d%m%Y_%H%M"), stride, batch_size))
    os.mkdir(dir_save)

    from progress.bar import Bar
    bar = Bar('Processing', max=len(batch_arr))

    for idx, batch in enumerate(batch_arr):
        df = build_batch(columns=parser.columns, batch=batch)
        save_df_as_parquet(df=df,dir_save=dir_save,
                           save_name="batch{}.parquet.snappy".format(idx))
        bar.next()

    return "All batches written to parquet."

if __name__=="__main__":

    files = get_files()
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
        "ORIGINAL_NUTS_CODE","DURATION", "DURATION_TYPE",
        "AWARD_CONTRACT_ITEM", "AC_AWARD_CRIT", "AC_AWARD_CRIT_CODE",
        "ISO_COUNTRY_VALUE", "VALUE_CURRENCY", "VAL_ESTIMATED_TOTAL_CURRENCY"]
    sum_list = ["VALUE", "VAL_TOTAL", "VAL_ESTIMATED_TOTAL", "VALUE_COST",
        "VALUE_COST_FMTVAL"]

    parser = XmlParser(columns=maj_vote_list+sum_list)
    parse_and_write(files=files,columns=parser.columns,stride=1)


#    (DfBuilder(stride=1000, columns_of_interest=maj_vote_list+sum_list).batch_build_and_write(500))
