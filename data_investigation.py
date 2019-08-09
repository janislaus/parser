from pyspark.sql.functions import *
#start sparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
# load df
df = spark.read.parquet("ted_bulk/parquet/df_every_1000th_file_p2.parquet.gzip")

#cpv-codes
df.select("CPV_CODE_CODE", "ORIGINAL_CPV_CODE", "ORIGINAL_CPV").show()
# nuts
df.select("NUTS_CODE", "TENDERER_NUTS", "ORIGINAL_NUTS", "TENDERER_NUTS_CODE", "ORIGINAL_NUTS_CODE").show()
# duration
df.select("DURATION", "DURATION_TENDER_VALID_TYPE", "DURATION_TYPE", "DURATION_TENDER_VALID", "DURATION_FRAMEWORK_YEAR", "DURATION_FRAMEWORK_MONTH").show()
df.select("DURATION", "DURATION#4","DURATION#3","DURATION#1","DURATION#2", "DURATION#5", "DURATION#6","DURATION#7","DURATION#8","DURATION#9").where(col("DURATION").isNotNull()).show(100)
#was ist value --> wahrscheinlich der kosten für ein item
df.select("VALUE", "VALUE#4","VALUE#3","VALUE#1","VALUE#2", "VALUE#5", "VALUE#6","VALUE#7","VALUE#8","VALUE#9").where(col("VALUE").isNotNull()).show(100)

df.select("HIGH_VALUE", "HIGH_VALUE_FMTVAL", "INITIAL_ESTIMATED_TOTAL_VALUE_CONTRACT_CURRENCY", "LOW_VALUE", "LOW_VALUE_FMTVAL", "VALUES_PUBLICATION", "VALUES_TYPE", "VALUE_COST", "VALUE_COST_FMTVAL", "VALUE_CURRENCY").show(100)

df.select("E_MAIL", "E_MAIL#4","E_MAIL#3","E_MAIL#1","E_MAIL#2", "E_MAIL#5", "E_MAIL#6","E_MAIL#7","E_MAIL#8","E_MAIL#9").where(col("E_MAIL").isNotNull()).show(100)


COUNTRY_VALUE

df.select(col("BLK_BTX")).where(col("BLT_BTX").isNotNull()).show(100)
