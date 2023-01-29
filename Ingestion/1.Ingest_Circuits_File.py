# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits.csv File

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Read CSV file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_datasource","")
v_datasource=dbutils.widgets.get("p_datasource")

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

Circuits_Schema = StructType([StructField("circuitId",IntegerType(),False),
                              StructField("circuitRef",StringType(),True),
                              StructField("name",StringType(),True),
                              StructField("location",StringType(),True),
                              StructField("country",StringType(),True),
                              StructField("lat",DoubleType(),True),
                              StructField("lng",DoubleType(),True),
                              StructField("alt",IntegerType(),True),
                              StructField("url",StringType(),True)
                             ])
                              
                             

# COMMAND ----------

circuits_df=spark.read \
.option("header",True)\
.schema(Circuits_Schema) \
.csv('/mnt/formula1blob/raw/circuits.csv',header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: selecting the columns

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),
                               col("circuitRef"),
                               col("name"),
                               col("location"),
                               col("country"),
                               col("lat"),
                               col("lng"),
                               col("alt"),
                               col("url")                    
                              )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: columns renamed

# COMMAND ----------

circuits_renamed_df = (circuits_selected_df.withColumnRenamed("circuitId","circuits_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")
                      )

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Step 4: Add new audit columns

# COMMAND ----------

circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

circuits_final_df=add_data_source(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 5: Write data to processed layer

# COMMAND ----------

circuits_final_df.write\
.mode("overwrite")\
.format("parquet")\
.saveAsTable("f1_processed.circuits")


# COMMAND ----------

dbutils.notebook.exit("success")
