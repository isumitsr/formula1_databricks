# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the csv file using the spark dataframe Reader API

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/formula1dlsc/raw

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("location", StringType(), True),
                                       StructField("country", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng", DoubleType(), True),
                                       StructField("alt", IntegerType(), True),
                                       StructField("url", StringType(), True)

])

# COMMAND ----------

# load the csv file to dataframe
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## selecting only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"),col("lng"),col("alt")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding a new column - ingestion_date

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
  #  .withColumn("env", lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write data to DataLake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# %fs
# ls /mnt/formula1dlsc/processed/circuits

# COMMAND ----------

dbutils.notebook.exit("Successful")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits
