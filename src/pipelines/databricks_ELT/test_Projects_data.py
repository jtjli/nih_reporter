# Databricks notebook source
import zipfile
import os.path
import pandas as pd

# 
#f="/dbfs/FileStore/nih/raw_data/downloaded_230325/Projects/RePORTER_PRJ_C_FY1986.zip"
f="/dbfs/FileStore/nih/raw_data/downloaded_230325/Projects/RePORTER_PRJ_C_FY2004.zip"
os.path.exists(f)
zf = zipfile.ZipFile(f)
zf.namelist()


#
# AVOID CUSTOM PARSING
## extract a specific file from the zip container
#csv = zf.open(zf.namelist()[0])
## save the extraced file 
#head=csv.readline().rstrip().decode('utf8').split(',')
#line1=csv.readline().rstrip().decode('utf8').split(',')

#
# Use Pandas instead!!  (spark.read.csv does not support zip file!!)
csv = pd.read_csv(zf.open(zf.namelist()[0]))#,nrows=2)
#display( [("LENGTH",),(len(csv.index),)] )
print(f"nRows: {len(csv.index)}\n\nColumn names:")
print(csv.columns)


display(csv.head())




# COMMAND ----------

#df = spark.read.csv(zf)
df = spark.createDataFrame(csv)


# COMMAND ----------

display(df.select('SUBPROJECT_ID').distinct())
display(df.select('SUPPORT_YEAR').distinct())
display(df.select('FY').distinct())

# COMMAND ----------

df.schema
#df.printSchema()


# COMMAND ----------

display(df)



# COMMAND ----------

# MAGIC %run ./schema

# COMMAND ----------

#df2 = spark.createDataFrame(csv, schema=projects_schema)
df2 = spark.createDataFrame(csv)


# COMMAND ----------

df2.schema

# COMMAND ----------

# Try change date column, then reinforce new schema

print([x[0] for x in df2.select('PROJECT_START').distinct().collect()])

# COMMAND ----------

# Test pyspark basics:  to_date and col & withColumn

from pyspark.sql import functions as F

#F.to_date('05/15/1985','MM/dd/yyyy')
df3 = df2.withColumn('PROJECT_START_DT', F.to_date(F.col('PROJECT_START'),'MM/dd/yyyy'))

print(len(df2.columns))
print(len(df3.columns))


df3.select(['PROJECT_START','PROJECT_START_DT']).distinct().collect()

# COMMAND ----------

# Test write as new schema

from pyspark.sql import functions as F

#F.to_date('05/15/1985','MM/dd/yyyy')
df3 = df2.withColumn('PROJECT_START', F.to_date(F.col('PROJECT_START'),'MM/dd/yyyy'))

df3.select(['PROJECT_START']).distinct().take(10)

df3.schema

# COMMAND ----------

# Test manually adjust Schema

from pyspark.sql.types import *

ss = df3.schema.fields.pop(0)

assert ss.name == 'APPLICATION_ID'

df3.schema.fields.insert(0,StructField('APPLICATION_ID', StringType(), False)) #LongType(), False
print(df3.schema)

# COMMAND ----------

# Check which one is Date

for cn in df3.columns:

    display(df3.select(cn).distinct().take(10))

#BUDGET_START, BUDGET_END, PROJECT_START, PROJECT_END