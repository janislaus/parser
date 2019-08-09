# builds dataframe with data from bulk (from ted europe) from single rows (which
# are obtained from each xml file)

from parser2 import *
import pandas as pd
import glob, os
import pyarrow

# directory of where data of bulk is stored
# maj_vote_list:
# AC_CRITERION, AC_WEIGHTING?, ADDITIONAL_INFORMATION_ABOUT_LOTS?, AWARD_CONTRACT_ITEM?, ADDRESS?,
# sum_list:
#
class Df:

    def __init__(self, dir = "/home/janislaus/ted_bulk/data",
                save_dir = 'parquet/df_every_1000th_file_p2.parquet.gzip'):

        self.dir =  dir
        self.save_dir = save_dir
        self.xml_arr = []

    def get_xml_files(self):
        # store all links to the different xml files into list
        for filename in glob.iglob(self.dir + '/**', recursive=True):
            if os.path.isfile(filename): # filter dirs
                self.xml_arr.append(filename)

        return self


    def build_df(self, stride = 1, agg = False):

        sparse_arr = self.xml_arr[0::stride]
        # initialize Dataframe which will be build up gradually
        df = pd.DataFrame()

        # build up Dataframe row for row
        i = 0
        l = []
        for xml in sparse_arr:

            if (i%100 == 0 and i!=0):
                print("{}/{} lines added".format(i,len(sparse_arr)))
            i +=1
            if(agg):
                new_tender = Tender(xml).get_data().agg_data()
                df2 = pd.DataFrame(new_tender.data_dict)
            else:
                new_tender = Tender(xml).get_data()
                ndict = {item[0]: [item[1]] for item in new_tender.data}
                df2 = pd.DataFrame(ndict)

            df = pd.concat([df, df2], axis=0, ignore_index=True)

        # save dataframe as parquet file
        df.to_parquet(self.save_dir, compression='gzip')
        return "WOGALO"


if __name__=="__main__":
    maj_vote_list = ["CPV_CODE_CODE", "CPV_SUPPLEMENTARY_CODE_CODE", "ORIGINAL_CPV",
    "ORIGINAL_CPV_CODE"]
    sum_list = []
    Df().get_xml_files().build_df(stride = 1000)

# CPV_CODE_CODE = int
# ORIGINAL_CPV = text, bsp: Supporting services
# CPV_SUPPLEMENTARY_CODE_CODE: nur wenige einträge, aber vielleicht als
# ergänzung trotzdem interessant
# ORIGINAL_CPV_CODE = int, WICHTIG: 1/3 der nummern unterscheiden sich von
# CPV_CODE_CODE

#alles zu nuts
#["NUTS_CODE", "TENDERER_NUTS", "ORIGINAL_NUTS", "TENDERER_NUTS_CODE", "ORIGINAL_NUTS_CODE"]

#Duration: hier sind folgende 2 columns wichtig, majority auf beide
#["DURATION", "DURATION_TYPE"]

#wenns ums geld geht: VALUE und VAL_CURRENCY sind gut, VALUE_COST und
#VALUE_COST_FMTVAL vl als ergänzung
# VAL_TOTAL scheint oft den wert von VALUE zu haben und manchmal auch als
# einziges --> ein coalesce würde sich hier anbieten

#vergabestelle, einauftraggeber usw.
# CA_CE_NUTS = Ort, mal stadt mal land mal region
# CA_TYPE_OTHER = Forschungsgesellschaft, städtische gesellschaft, ...
# CA_TYPE_VALUE = nur wenige werte ~5: body_public, regional_authority, regional
#                 agency, national_agency, ministry
# CONTACT_POINT = oft personen, manchmal einrichtungen
# IA_URL_ETENDERING = 50%, urls sehen so aus als könnten sie rückschlüsse
# zulassen
# IA_URL_GENERAL = of wie etendering dings aber auch oft anders, interessant
# MA_MAIN_ACTIVITIES = 100%, so sachen wie: DEFENCE |ECONOMIC_AND_FINANCIAL_AFFAIRS |EDUCATION |ENVIRONMENT
# |GENERAL_PUBLIC_SERVICES |HEALTH |HOUSING_AND_COMMUNITY_AMENITIES
# |PUBLIC_ORDER_AND_SAFETY |RECREATION_CULTURE_AND_RELIGION |SOCIAL_PROTECTION
# MA_MAIN_ACTIVITIES_CODE = 100%, einträge wie: C, S, L, 8 usw --> wenn man hier
# weiß wofür das steht, könnte das ganze sehr interessant werden
# NOTICE_NUMBER_OJ = 50%, einträge der art: 2017/S 237-492799, könnte
# interessant sein
# NO_DOC_OJS = 100% , gleiche art von einträgen wie NOTICE_NUMBER_OJ
# OFFICIALNAME = 2/3, sieht zum großteil nach auftraggeber aus, zb: DB
# Engineering, Centrum Medyczne, ...


#!!! COUNTRY_VALUE = kürzel für land: RO, NL, DE, ... und ungefähr 2/3 notnull
# zusätzlich noch: LANGUAGE_VALUE (aber nicht soviele notnulls)
# zusätzlich noch: NATIONALID ~50%, aber seltsame zahlencodes zb: 24830054300217

# CRITERIA = wahrscheinlich das kriterium für welches angebot gewinnt

# angaben rund um zeitangaben:
# [DATE, DATE_CONCLUSION_CONTRACT, DATE_DISPATCH_NOTICE, DATE_END, DATE_START,
# DATE_PUBLICATION_NOTICE, DELETION_DATE]
# all die dinger sind super unregelmäßig ausgefüllt
# !! außer DELETION_DATE, das gibt es immer
# MONTH = gibt monat an, zu was genau nie wiem, 10%

# E_MAIL = irgendwelche persönlichen addressen --> bringt nichts

# FAX: ziemlich oft da ~50% --> vielleicht hier irgendein mapping möglich

# MAIN_SITE: ~25%, möglicherweise interessante einträge, mal Ort, mal Institution usw...

# NB_TENDERS_RECEIVED_* (hier gibts ein paar sehr ähnlich col names)= int, vl
# anzahl der angebote für das tender? ~40%,

# NO_OJ = int, 100%, keine ahnung was das soll

# OPTIONS_DESCR: <10%, sieht nach fließtext aus
