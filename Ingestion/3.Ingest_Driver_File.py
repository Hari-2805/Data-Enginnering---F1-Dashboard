# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Ingest Driver.JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Read the JSON file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StructField, StringType, DateType
from pyspark.sql.functions import current_timestamp,concat,lit

# COMMAND ----------

dbutils.widgets.text("p_datasource","Ergast_API")
v_datasource=dbutils.widgets.get("p_datasource")

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(),True),
                          StructField("surname", StringType(),True)
                         ])

# COMMAND ----------

driver_schema =  StructType([StructField("driverId", IntegerType(),False),
                             StructField("driverRef", StringType(),True),
                             StructField("number", IntegerType(),True),
                             StructField("code", StringType(), True),
                             StructField("name", name_schema),
                             StructField("dob", DateType(), True),
                             StructField("nationality", StringType(),True),
                             StructField("url", StringType(), True)])

# COMMAND ----------

df = spark.read\
.schema(driver_schema)\
.json("/mnt/formula1blob/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Rename columns and add new columns

# COMMAND ----------

drivers_renamed_df = df.withColumnRenamed("driverId", "driver_id")\
                       .withColumnRenamed("driverRef", "driver_ref")\
                       .withColumn("ingestion_method",current_timestamp())\
                       .withColumn("name", concat(df.name.forename, lit(' '), df.name.surname))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

drivers_final_df=add_data_source(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: Write ouput to parquet file

# COMMAND ----------

drivers_final_df.write\
.mode("overwrite")\
.format("parquet")\
.saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("success")
