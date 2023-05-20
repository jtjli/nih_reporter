# Databricks notebook source
testdf1 = spark.createDataFrame([],"myint INT NOT NULL, mystr STRING")

mycolnames = ["X","SS"]
testdf1b = spark.createDataFrame([],"myint INT NOT NULL, mystr STRING").toDF(*mycolnames)


# COMMAND ----------

testdf1.schema

# COMMAND ----------

testdf1.collect()


# COMMAND ----------

testdf2 = spark.createDataFrame([ [2,"a"], [3,"b"]])
testdf2b = testdf2.toDF(*mycolnames)


# This will lead to failure: "Cannot perform Merge as multiple source rows matched and attempted to modify the same"
testdf3 = spark.createDataFrame([ [3,"b"], [3,"bb"], [4,"cc"]])
testdf3b = testdf3.toDF(*mycolnames)


# COMMAND ----------

# create temp views (with an intention to create Tables/ use Table features - in this case, merge)

testdf1.createOrReplaceTempView('v_testdf1')
testdf1b.createOrReplaceTempView('v_testdf1b')

testdf2.createOrReplaceTempView('v_testdf2')
testdf2b.createOrReplaceTempView('v_testdf2b')

testdf3.createOrReplaceTempView('v_testdf3')
testdf3b.createOrReplaceTempView('v_testdf3b')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `v_testdf2b`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE t_testdf1b AS
# MAGIC SELECT * FROM v_testdf1b

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE FORMATTED t_testdf1b

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/t_testdf1b/_delta_log')

# COMMAND ----------

spark.sql("""
    MERGE INTO t_testdf1b USING v_testdf2b
    ON t_testdf1b.X = v_testdf2b.X
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM t_testdf1b

# COMMAND ----------

spark.sql("""
    MERGE INTO t_testdf1b USING v_testdf3b
    ON t_testdf1b.X = v_testdf3b.X
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM t_testdf1b

# COMMAND ----------

#testdf2b.merge(testdf1b,"testdf1b.X = testdf2b.X")


# (targetDF
#   .merge(sourceDF, "source.key = target.key")
#   .whenMatchedUpdateAll()
#   .whenNotMatchedInsertAll()
#   .whenNotMatchedBySourceDelete()
#   .execute()
# )

# deltaTablePeople.alias('people') \
#   .merge(
#     dfUpdates.alias('updates'),
#     'people.id = updates.id'
#   ) \