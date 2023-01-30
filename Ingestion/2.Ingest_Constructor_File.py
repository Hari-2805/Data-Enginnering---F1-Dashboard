# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Ingest constructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Read the constructor JSON file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_datasource","")
v_datasource=dbutils.widgets.get("p_datasource")

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

#Tried with method 1 to define the schema using struct method

constructor_schema_test = StructType([StructField("constructorId", IntegerType(), False),
                                      StructField("constructorRef", StringType(), True),
                                      StructField("name", StringType(),True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(),True)
                                     ])

# COMMAND ----------

constructor_schema = ("constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING")

# COMMAND ----------

df=spark.read \
.schema(constructor_schema) \
.json("/mnt/formula1blob/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Rename columns and add audit column

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
                                             .withColumnRenamed("constructorRef","constructor_Ref") \
                                             .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

constructor_final_df=add_data_source(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Step 4: Write the output to parquet file

# COMMAND ----------

constructor_final_df.write\
.mode("overwrite")\
.format("parquet")\
.saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")
