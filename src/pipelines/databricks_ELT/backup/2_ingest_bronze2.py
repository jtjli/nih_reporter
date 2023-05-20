# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 1. Create ~LIVE~ DELTA TABLE - Projects, Abstracts, etc
# MAGIC 2. Create dated snapshots, for research use

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Set VARIABLES in spark conf so that they can be used across SQL, Python, etc.
# MAGIC data_root = '/FileStore/nih/raw_data/downloaded_230325'
# MAGIC data_projects_path = f"{data_root}/Projects_unzipped"
# MAGIC data_abstracts_path = f"{data_root}/Abstracts_unzipped"
# MAGIC
# MAGIC spark.conf.set ('nih.path.root', f"{data_root}")
# MAGIC spark.conf.set ('nih.path.projects', data_projects_path)
# MAGIC spark.conf.set ('nih.path.abstracts', data_abstracts_path)

# COMMAND ----------

# spark dataframes are useful for analytics, but its partitioning mechanism can complicates things such as "dropDuplicates"
# https://stackoverflow.com/questions/38687212/spark-dataframe-drop-duplicates-and-keep-first
#
# It can be beneficial to use pandas when you know you are dealing with small data
#
#   Here, we initialize the result table as a Delta Table (so that MERGE can be used), which will get updated as each file is parsed. 
#   Each individual file is managed using pandas dataframe, such as ensuring unique application IDs are present. Then saved as a TempView
#   which will be used for the UPSERT MERGE action.
#
# NOTES:
#   Upsert in databricks can only be applied to Delta Table (master), not spark dataframes. The update tables can be DF (python) or TempViews (SQL)

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

def mergeToMaster(master_lbl, update_pd_df, schema):
    updatedf = spark.createDataFrame(update_pd_df,schema)
    updatedf.createOrReplaceTempView('v_updatedf')
    spark.sql(f"""
        MERGE INTO {master_lbl} USING v_updatedf
        ON {master_lbl}.APPLICATION_ID = v_updatedf.APPLICATION_ID
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# FUNCTION to process a single project csv - check unique IDs, convert dates, then do DeltaTable MERGE

import zipfile
import os.path
#import pandas as pd
import pyspark.pandas as pd

#
# Note - spark or pyspark.pandas do NOT support reading from zip file directly. Using pandas here instead.

def process(f):
    # zffp = f"/dbfs{data_projects_path}/{f}"
    # zf = zipfile.ZipFile(zffp)
    # csvf = zf.namelist()
    # if len(csvf) > 1:
    #     print (f"SKIPPING {f}: {csvf}")
    #     return

    # assert len(csvf) == 1, f"{f} {csvf}"
    # csvf = csvf[0]
    f_fp = f"{data_projects_path}/{f}"
    csv = pd.read_csv(f_fp)

    # Remove duplicates (keep the last one)
    dups = csv.duplicated("APPLICATION_ID", keep='last')
    if (sum(dups) > 0):
        print( f"Duplicates found: {f} - {csv['APPLICATION_ID'][dups].tolist()}")
    csv2 = csv[~dups]

    # Convert date columns to DateType
    for cn in projects_date_fields:
        csv2[cn] = pd.to_datetime(csv2[cn],format='%m/%d/%Y')

    # Rearrange columns to match schema columns (columns across files can differ)
 
    assert set(csv2.columns.tolist()) == set(projects_schema.names), f"Unequal column names for {f} {csv2.columns}"
    csv2 = csv2[projects_schema.names]

    # Merge into master table
    mergeToMaster("Projects", csv2, projects_schema)


#process(fs[1])

# COMMAND ----------

         # to see if normal pandas show corrupted PROJECT_START as well


# import pandas as pd
import pyspark.pandas as pd
f=fs[6]
f=fs[23]


f
import pandas
f_fp2 = f"/dbfs{data_projects_path}/{f}"
csvpd = pandas.read_csv(f_fp2, dtype=str)
pscol = csvpd["PROJECT_END"].astype(str)
#csvpd.dtypes
pscollen=[len(x) for x in pscol] #.unique()
print("READ FROM NORMAL PANDAS")
display(set(pscollen))

import pyspark.pandas as pd
f_fp = f"{data_projects_path}/{f}"
csv = pd.read_csv(f_fp, dtype=str)
print(csv.shape)
print("\nREAD FROM PYSPARK PANDAS")

display(set([len(x) for x in csv["PROJECT_END"].astype(str).to_numpy()]))

from pyspark.sql import functions as F
#csvSK = #spark.read.csv(f_fp,inferSchema=False,header=True,sep=',') # force all types to strings
csvSK = (
  spark.read.format('csv')
  .option("header", "true")
  .option("inferSchema", "false")
  .option("quote", "\"")
  .option("escape", "\"")
  .option("escapeQuotes", "true")
  .option("multiLine", "true")
  .load(f_fp)
)
print((csvSK.count(),len(csvSK.columns)))
csvSK.columns
csvSK.schema
csvSK2 = csvSK.withColumn('Len',F.length(F.col("PROJECT_END")))

print("\nREAD FROM SPARK")

display(csvSK2.select('Len').distinct())
csvSK2.schema

extDF= csvSK2.filter(F.col('Len') > 10).limit(5)

for cn in extDF.columns:
    print (cn)
    display(extDF.select(cn))


# COMMAND ----------

# TEST CODES

import zipfile
import os.path
import numpy as np
# import pandas as pd
import pyspark.pandas as pd
f=fs[6]
f=fs[23]

# zffp = f"/dbfs{data_projects_path}/{f}"
# zf = zipfile.ZipFile(zffp)
# csvf = zf.namelist()
# csvf = csvf[0]
# csv = pd.read_csv(zf.open(csvf))
# display(csv)


f_fp = f"{data_projects_path}/{f}"
csv = pd.read_csv(f_fp, dtype=str)
csv.head()

# csv["APPLICATION_TYPE"].astype('Int32')
# csv["APPLICATION_TYPE"].dtypes
# aa=csv["APPLICATION_TYPE"].tolist()
# aa.sort(reverse=True)
# print(aa)
# [int(a) for a in aa if not np.isnan(a)]

# COMMAND ----------

csv["PROJECT_START"].unique().map(len).unique()

# COMMAND ----------

from pyspark.sql import functions as F

csv_sk = csv.to_spark()
#cn = 'BUDGET_START'
for cn in projects_date_fields:
    print(cn)
    unique_dates = csv_sk.select(cn).distinct()
    csv_sk2 = csv_sk.withColumn('Len',F.length(F.col(cn)))
    display(csv_sk2.select([cn,'Len']).distinct().sort('Len').take(10))
    display(csv_sk2.select([cn,'Len']).distinct().sort('Len',ascending=False).take(10))



# COMMAND ----------

# TEST CODES

pd.set_option('compute.ops_on_diff_frames', True)

#csv[~csv['NIH_SPENDING_CATS'].isnull()]
dups = csv.duplicated("APPLICATION_ID", keep='last')
csv.head()

# testdatetype1 = csv['PROJECT_END'].map(lambda x: '-' in x)
# testdatetype2 = csv['PROJECT_END'].map(lambda x: '/' in x)
# sum(testdatetype1.to_numpy())
# sum(testdatetype2.to_numpy())

#if (sum(dups) > 0):
#dups.cast("long")
dups.to_numpy()
if (sum(dups.to_numpy()) > 0):
    print( f"Duplicates found: {f} - {csv['APPLICATION_ID'][dups].tolist()}")
csv2 = csv[~dups]
csv2.head()
assert csv2.shape[0] > 0
def cnvert_date2(d):
    if '-' in d:
        return pd.to_datetime(d,format='%Y-%m-%d')
    elif '/' in d:
        #assert '/' in d, f" '/' not in d {d}"
        return pd.to_datetime(d,format='%m/%d/%Y')
    else:
        print(f"PROBLEM d for cnvert_date {d}")
        return np.nan


# d='2008-01-28'
# d = '1/28/2008'
# cnvert_date(d)

for cn in projects_date_fields:
    print(cn)
    csv2[cn] = csv2[cn].map(cnvert_date2)

cntmp = pd.Series( ['2008-01-28','2007-03-31'])
cntmp.map(cnvert_date)
# # Convert date columns to DateType
# for cn in projects_date_fields:
# #    print(type(csv2.loc[0,projects_date_fields[0]]))
#     if '-' in csv2.loc[0,cn]:
#         csv2[cn] = pd.to_datetime(csv2[cn],format='%Y-%m-%d')
#     else:
#         csv2[cn] = pd.to_datetime(csv2[cn],format='%m/%d/%Y')

#csv2.columns

csv2[projects_date_fields[0]]
csv2[~csv2['NIH_SPENDING_CATS'].isnull()]

csv.loc[:,'BUDGET_START'][0]

csv2.loc[:,'BUDGET_START'][0]

# Merge into master table
#mergeToMaster("Projects", csv3, projects_schema)


# COMMAND ----------

# # TEST CODES
# csv2.iloc[0:2,10:]

# COMMAND ----------

# # TEST CODES
# len(projects_schema.fields)

# COMMAND ----------

# # TEST CODES
# print( list(zip(csv2.columns,projects_schema.fieldNames())))
# #csv2.columns.to_list()
# #projects_schema.fieldNames()

# COMMAND ----------

# TEST CODES

#def mergeToMaster(master_lbl, update_pd_df, schema):

master_lbl = "Projects"
update_pd_df = csv2

schema = projects_schema


print(f"SHAPE: {update_pd_df.shape},  CONTENT OF NIH_SPENDING_CATS: {update_pd_df['NIH_SPENDING_CATS'].unique()}")

# [CONTHERE]
updatedf = spark.createDataFrame(update_pd_df,schema)
updatedf.createOrReplaceTempView('v_updatedf')        
spark.sql(f"""
    MERGE INTO {master_lbl} USING v_updatedf
    ON {master_lbl}.APPLICATION_ID = v_updatedf.APPLICATION_ID
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# TEST CODES

# csv2['NIH_SPENDING_CATS'].unique()
# csv['SUBPROJECT_ID'][10]
# csv['SUPPORT_YEAR'][9]

# COMMAND ----------

# TEST CODES

# len(csv["APPLICATION_ID"].unique()) == csv.shape[0]

# dups = csv.duplicated("APPLICATION_ID", keep='last')
# csv2 = csv[~dups]
# csv2.shape
# #print( f"{csv2['APPLICATION_ID'][~dups].tolist()}")

# #csv2['PROJECT_START'].unique().tolist()

# #csv2['PROJECT_START22'] = pd.to_datetime(csv2['PROJECT_START'],format='%m/%d/%Y')
# for cn in projects_date_fields:
#     csv2[cn] = pd.to_datetime(csv2[cn],format='%m/%d/%Y')

# display(csv2)


# COMMAND ----------

# TEST CODES

# test_spark = spark.createDataFrame(csv2,projects_schema)
# display(test_spark)
# test_spark.schema

# COMMAND ----------

#
# Loop through and process all CSVs
#


for f in fs:
    print(f)
    process(f)


