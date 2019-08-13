from pyspark.sql.functions import *
#start sparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
# load df
df = spark.read.parquet("parquet/df_every_100th_file_p2.parquet.gzip")

#cpv-codes
df.select("CPV_CODE_CODE", "ORIGINAL_CPV_CODE", "ORIGINAL_CPV").show()
# nuts
df.select("NUTS_CODE", "TENDERER_NUTS", "ORIGINAL_NUTS", "TENDERER_NUTS_CODE", "ORIGINAL_NUTS_CODE").show()
# duration
df.select("DURATION", "DURATION#4","DURATION#3","DURATION#1","DURATION#2",
"DURATION#5",
"DURATION#6","DURATION#7","DURATION#8","DURATION#9").where(col("DURATION").isNotNull()).show(100)
#was ist value --> wahrscheinlich der kosten für ein item
df.select("VALUE", "VALUE#4","VALUE#3","VALUE#1","VALUE#2", "VALUE#5",
"VALUE#6","VALUE#7","VALUE#8","VALUE#9").where(col("VALUE").isNotNull()).show(100)

df.select("HIGH_VALUE", "HIGH_VALUE_FMTVAL",
"INITIAL_ESTIMATED_TOTAL_VALUE_CONTRACT_CURRENCY", "LOW_VALUE",
"LOW_VALUE_FMTVAL", "VALUES_PUBLICATION", "VALUES_TYPE", "VALUE_COST",
"VALUE_COST_FMTVAL", "VALUE_CURRENCY").show(100)

for cl in df.columns:
    print(cl + ": " + str(float(df.where(col(cl).isNotNull()).count())/float(df.count()))   )

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
#VALUE_COST_FMTVAL, VAL_ESTIMATED_TOTAL vl als ergänzung
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
# URL = 15%
# URL_BUYER und URL_GENERAL sind gut
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
# TI_DOC = 0.5%, CountryCode dann City dann nach irgendwas: PL-Szczecin: Tone...


#!!! COUNTRY_VALUE = kürzel für land: RO, NL, DE, ... und ungefähr 2/3 notnull
#!!! ISO_COUNTRY_VALUE noch besser
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

# POSTAL_CODE = 2/3, vl noch für irgendwas nützlich

# RECEPTION_ID = 100% irgendwelche ID's: 18-131844-001

# SHORT_DESCR = 2/3, fließtext

# TED_EXPORT_DOC_ID = unique id for each row !

# TITLE = 2/3 title of each tender! gar nicht mal so uninteressant

# TOWN = 2/3, Warszawa, Stockholm, ...

# TYPE_CONTRACT_CTYPE = 2/3, drei verschiedene Werte: services, works, supplies
