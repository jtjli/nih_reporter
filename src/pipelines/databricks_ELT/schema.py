# Databricks notebook source
#
# Best practice is to know your data and declare your schema explicity.
# Schema was first inferred from data, but then manually assessed and adjusted to create the following schemas.
#

#
# adapted from inferred schema in peek_zip.py
#

from pyspark.sql.types import *

projects_schema = StructType(
    [
        StructField("APPLICATION_ID", LongType(), False),    # Use this as ID; Nullable set to False
        StructField("ACTIVITY", StringType(), True),
        StructField("ADMINISTERING_IC", StringType(), True),
        StructField("APPLICATION_TYPE", LongType(), True),
        StructField("ARRA_FUNDED", DoubleType(), True),
        StructField("AWARD_NOTICE_DATE", StringType(), True),   # Different format to other dates; leave as String
        StructField("BUDGET_START", DateType(), True),    #Date
        StructField("BUDGET_END", DateType(), True),      #Date
        StructField("CFDA_CODE", DoubleType(), True),
        StructField("CORE_PROJECT_NUM", StringType(), True),
        StructField("ED_INST_TYPE", StringType(), True),
        StructField("FOA_NUMBER", StringType(), True),
        StructField("FULL_PROJECT_NUM", StringType(), True),
        StructField("SUBPROJECT_ID", DoubleType(), True),   # IDs .. has NaNs
        StructField("FUNDING_ICs", StringType(), True),     # String
        StructField("FY", StringType(), True),        # Year/Int; LEAVE AS STRING -- contaminated entries
        StructField("IC_NAME", StringType(), True),
        StructField("NIH_SPENDING_CATS", DoubleType(), True),
        StructField("ORG_CITY", StringType(), True),
        StructField("ORG_COUNTRY", StringType(), True),
        StructField("ORG_DEPT", StringType(), True),
        StructField("ORG_DISTRICT", DoubleType(), True),
        StructField("ORG_DUNS", StringType(), True),
        StructField("ORG_FIPS", StringType(), True),
        StructField("ORG_NAME", StringType(), True),
        StructField("ORG_STATE", StringType(), True),
        StructField("ORG_ZIPCODE", StringType(), True),
        StructField("PHR", DoubleType(), True),
        StructField("PI_IDS", StringType(), True),
        StructField("PI_NAMEs", StringType(), True),
        StructField("PROGRAM_OFFICER_NAME", StringType(), True),
        StructField("PROJECT_START", DateType(), True),       #Date
        StructField("PROJECT_END", DateType(), True),         #Date
        StructField("PROJECT_TERMS", StringType(), True),
        StructField("PROJECT_TITLE", StringType(), True),
        StructField("SERIAL_NUMBER", LongType(), True),
        StructField("STUDY_SECTION", StringType(), True),
        StructField("STUDY_SECTION_NAME", StringType(), True),
        StructField("SUFFIX", StringType(), True),
        StructField("SUPPORT_YEAR", StringType(), True),   # Year/Int; LEAVE AS STRING -- contaminated entries
        StructField("TOTAL_COST", DoubleType(), True),
        StructField("TOTAL_COST_SUB_PROJECT", DoubleType(), True),
    ]
)

# COMMAND ----------


# Replaced by getFieldsByType
# projects_date_fields = []
# for field in projects_schema.fields:
#     if str(field.dataType) == 'DateType()':
#         projects_date_fields.append(field.name)

def getFieldsByType(schema1 : StructType, type1 : str):
    # type1: 'DateType()' | 'LongType()' | ...
    rfields = []
    for field in schema1.fields:
        if str(field.dataType) == type1:
            rfields.append(field.name)
    
    return rfields

projects_date_fields = getFieldsByType(projects_schema, 'DateType()')

# COMMAND ----------

# from pyspark.sql.types import *

# import pandas as pd
# import numpy as np
# # d = {'APPLICATION_ID': [1, 2], 'APPLICATION_TYPE': [3, np.nan]} #OK
# # d = {'APPLICATION_ID': [1, 2], 'APPLICATION_TYPE': [3, pd.NA]} #OK
# # d = {'APPLICATION_ID': [1, 2], 'APPLICATION_TYPE': [3, None]} #OK
# d = {'APPLICATION_ID': [1, 2], 'APPLICATION_TYPE': pd.Series([3, '']).astype('Int64') }
# df = pd.DataFrame(data=d)

# testschema = StructType(
#     [
#         StructField("APPLICATION_ID", LongType(), False),    # Use this as ID; Nullable set to False
#         StructField("APPLICATION_TYPE", LongType(), True),
#     ]
# )

# sdf = spark.createDataFrame(df,testschema)
# display(sdf)