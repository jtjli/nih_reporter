# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 1. Create ~LIVE~ DELTA TABLE - Projects, Abstracts, etc
# MAGIC 2. Create dated snapshots, for research use

# COMMAND ----------


# Set VARIABLES in spark conf so that they can be used across SQL, Python, etc.
data_root = '/FileStore/nih/raw_data/downloaded_230325'
data_projects_path = f"{data_root}/Projects_unzipped"
data_abstracts_path = f"{data_root}/Abstracts_unzipped"

spark.conf.set ('nih.path.root', f"{data_root}")
spark.conf.set ('nih.path.projects', data_projects_path)
spark.conf.set ('nih.path.abstracts', data_abstracts_path)

# COMMAND ----------

# spark dataframes are useful for analytics, but its partitioning mechanism can complicates things such as "dropDuplicates"
# https://stackoverflow.com/questions/38687212/spark-dataframe-drop-duplicates-and-keep-first
#
# For functions which Spark does not support, consider two options: 1) use pyspark.pandas -- spark dataframe can be converted to pyspark.pandas DF easily and you can have some additional functions.  2) Use normal pandas if pyspark.pandas fail you as well.
#
# Spark could not ingest "csvs with ""double quote"" followed by , so "  keep in mind it is relatively problemetic when compared to more mature and supported libraries like pandas.
#
# It can be beneficial to use pandas when you know you are dealing with small data. Use it to preprocess, prior to converting them into spark df. The only problem will be type casting. If needs debug, test it column by column.
#
#   Here, we initialize the result table as a Delta Table (so that MERGE can be used), which will get updated as each file is parsed. 
#   Then saved as a TempView which will be used for the UPSERT MERGE action.
#
# NOTES:
#   Upsert in databricks can only be applied to Delta Table (master), not spark dataframes. The update tables can be a spark df or TempViews (SQL)

# COMMAND ----------

# MAGIC %run ./schema

# COMMAND ----------

# %sql
# -- Run this to replace table; first two lines are best practice before drop
# DELETE FROM Projects;
# VACUUM Projects;
# DROP TABLE Projects;

# -- VACUUM Projects DRY RUN  
# -- VACUUM Projects  

# COMMAND ----------


# CREATE THE MASTER 'Projects' TABLE

#
# Using python only
df1 = spark.createDataFrame([],projects_schema)
df1.write.saveAsTable("Projects",mode="ignore")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Projects
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Projects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT FUNDING_ICs FROM projects

# COMMAND ----------


detaildf = spark.sql("DESCRIBE DETAIL Projects")
display(detaildf)

# COMMAND ----------

# The list of all downloaded "Projects" files
fs = [x[1] for x in dbutils.fs.ls(data_projects_path)]

# Line up the files in ascending order of YEAR
fs.sort()
print(fs)


# COMMAND ----------

def mergeToMaster(master_lbl, update_df_spark):
    updatedf = update_df_spark
    updatedf.createOrReplaceTempView('v_updatedf')
    spark.sql(f"""
        MERGE INTO {master_lbl} USING v_updatedf
        ON {master_lbl}.APPLICATION_ID = v_updatedf.APPLICATION_ID
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# def mergeToMaster(master_lbl, update_pd_df, schema):
#     updatedf = spark.createDataFrame(update_pd_df,schema)
#     updatedf.createOrReplaceTempView('v_updatedf')
#     spark.sql(f"""
#         MERGE INTO {master_lbl} USING v_updatedf
#         ON {master_lbl}.APPLICATION_ID = v_updatedf.APPLICATION_ID
#         WHEN MATCHED THEN UPDATE SET *
#         WHEN NOT MATCHED THEN INSERT *
#     """)

# COMMAND ----------

import pyspark.pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.sql.types import *

def read_f(csv_fullpath):
    csvSK = (
    spark.read.format('csv')
    .option("header", "true")
    .option("inferSchema", "false")   # Put all into strings; then convert manually
    .option("quote", "\"")            # To fix bug: https://stackoverflow.com/questions/40413526/reading-csv-files-with-quoted-fields-containing-embedded-commas
    .option("escape", "\"")           # as above
    .option("escapeQuotes", "true")   # may not be needed
    .option("multiLine", "true")      # may not be needed
    .load(csv_fullpath)
    )

    print((csv_fullpath, csvSK.count(),len(csvSK.columns)))

    #
    # Ignoring some columns not found in earlier data
    #('/FileStore/nih/raw_data/downloaded_230325/Projects_unzipped/RePORTER_PRJ_C_FY2020.csv', 82359, 46) FOUND UNEXPECTED COLUMNS - IGNORING: ['FUNDING_MECHANISM', 'ORG_IPF_CODE', 'DIRECT_COST_AMT', 'INDIRECT_COST_AMT']
    #assert len(csvSK.columns) == 42
    if len(csvSK.columns) > 42:
        extra_cols = [x for x in csvSK.columns if x not in projects_schema.names]
        print (f"FOUND UNEXPECTED COLUMNS - IGNORING: {extra_cols}")

    #
    # If files are dumped from a relational database, then APPLICATION_ID should be unique
    assert csvSK.count() == csvSK.dropDuplicates(["APPLICATION_ID"]).count(), "Appication ID's not unique - not expected"

    #
    # In theory can detect duplicates and 'keep=last' (we are making assumptions on how input files are prepared). But it should be done prior to partitioning the data / preprocessed with pandas, because distributed dataframe don't have concepts of row orders
    #
    #https://kb.databricks.com/sql/gen-unique-increasing-values (not even sure if below is reliable... becaused its partitioned already... should preprocess with row_number prior to reading into spark.)
    #
    #csvSK = csvSK.rdd.zipWithIndex().toDF(inf)
    # csvSK = csvSK.withColumn("_uid", F.monotonically_increasing_id())
    # window = Window.orderBy(F.col('_uid'))
    # csvSK = csvSK.withColumn('UID', F.row_number().over(window))


    for pdf in projects_date_fields:
        cn=f"Len_{pdf}"
        csvSK = csvSK.withColumn(cn,F.length(F.col(pdf)))
        assert csvSK.agg({cn:'max'}).first()[0] <= 10

    return csvSK

#
# Below, setting errors = 'coerce' due to this: pandas._libs.tslibs.np_datetime.OutOfBoundsDatetime: Out of bounds nanosecond timestamp: 3012-04-30 00:00:00
# I.e. turn improper datetime to NA... (rather than trying to infer it as 2012 instead of 3012). In big data world, poor data entries are at best skipped/ignored... difficult to resurrect every data entry error
#
@F.udf(returnType=DateType())
def cnvert_date2(d):
    if not d:
        return None
    if '-' in d:
        ret= pd.to_datetime(d,format='%Y-%m-%d',errors="coerce")
    elif '/' in d:
        #assert '/' in d, f" '/' not in d {d}"
        ret= pd.to_datetime(d,format='%m/%d/%Y',errors="coerce")
    else:
        print(f"PROBLEM d for cnvert_date {d}")
        return None
    if str(ret) == 'NaT':
        return None
    else:
        return ret 

def transform_date_fields(df):
    for pdf in projects_date_fields:
        df = df.withColumn(pdf,cnvert_date2(F.col(pdf)))

    return df

# # Test 

# f = fs[23]

# f_fp = f"{data_projects_path}/{f}"
# sdf = read_f(f_fp)

# # sdf.select(F.max(F.col('Len'))).first()[0]
# # sdf.agg({'Len':'max'}).first()[0]


# COMMAND ----------

# # TEST CODES
# # change DATE(strings) to Date fields; mind the dtype of the updated field
# # borrow codes from below; use functions from above

# # Test 

# f = fs[24]

# f_fp = f"{data_projects_path}/{f}"
# sdf = read_f(f_fp)

# print(sdf.columns)

# sdf5 = sdf.transform(transform_date_fields)
# display(sdf5.limit(1000))

# display(sdf.limit(10).select(*projects_date_fields))

# sdf6=sdf5.select(*projects_schema.names)
# sdf6.columns
# assert set(sdf6.columns) == set(projects_schema.names), f"Unequal column names for {f} {csv2.columns}"
# csv2 = csv2[projects_schema.names]

# # Merge into master table
# mergeToMaster("Projects", csv2, projects_schema)


# # sdf.select(F.max(F.col('Len_BUDGET_START'))).first()[0]
# # sdf.agg({'Len':'max'}).first()[0]


# # spdf = sdf.pandas_api()
# # dups = spdf.duplicated("APPLICATION_ID", keep='last')

# # sdf2=sdf.withColumn("NotDup",~dups)
# # spdf['keep'] = ~spdf.duplicated("APPLICATION_ID", keep='last')
# # if (sum(dups.to_numpy()) > 0):
# #     print( f"Duplicates found: {f} - {spdf['APPLICATION_ID'][dups].tolist()}")
# # spdf2 = spdf.loc[~dups.to_numpy(),:]
# # spdf2 = spdf.loc[~spdf.duplicated("APPLICATION_ID", keep='last'),:]

# @F.udf(returnType=DateType())
# def cnvert_date2(d):
#     if '-' in d:
#         return pd.to_datetime(d,format='%Y-%m-%d')
#     elif '/' in d:
#         #assert '/' in d, f" '/' not in d {d}"
#         return pd.to_datetime(d,format='%m/%d/%Y')
#     else:
#         print(f"PROBLEM d for cnvert_date {d}")
#         return np.nan


# #
# # NOTES ON UDF: some arithemtics and vector operations can benefit from pandas_udf / apache arrow (https://arrow.apache.org/cookbook/py/)
# # But other logics can default to use SQL UDF (easier/ cleaner codes)
# cnvert_date_udf = F.udf(lambda z: cnvert_date2(z),StringType())

# @F.udf
# def test_udf2(z):
#     return z

# @F.udf
# def test_udf3(z):
#     return "HI"+z

# test_udf = F.udf(lambda z: z, StringType())
# sdf3 = sdf.withColumn("BUDGET_STARTTEST",test_udf3(F.col("BUDGET_START")))
# display(sdf3.select("BUDGET_START","BUDGET_STARTTEST").limit(10))

# display(sdf.select("*").limit(10))
# # CONTHERE fix transform
# sdf2 = sdf.withColumn("BUDGET_START2",cnvert_date2(F.col("BUDGET_START")))
# display(sdf2.select("BUDGET_START","BUDGET_START2").limit(10000))
# sdf2.count()
# len(sdf2.columns)
# sdf2.take(10)


# COMMAND ----------

def process(f):
    f_fp = f"{data_projects_path}/{f}"
    sdf = read_f(f_fp)
    sdf5 = sdf.transform(transform_date_fields)
    sdf6 = sdf5.select(*projects_schema.names)
 
    # Merge into master table
    mergeToMaster("Projects", sdf6)


# COMMAND ----------

#
# Loop through and process all CSVs
#

start_ind = 0 #24

#for f in fs:
for i in range(start_ind,len(fs)):
    f = fs[i]
    print(f)
    process(f)


