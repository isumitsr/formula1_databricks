# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races File

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(),True),
                                    StructField("round", IntegerType(),True),
                                    StructField("circuitId", IntegerType(),True),
                                    StructField("name", StringType(),True),
                                    StructField("date", DateType(),True),
                                    StructField("time", StringType(),True),
                                    StructField("url", StringType(),True),

])

# COMMAND ----------

races_df = spark.read \
.schema(races_schema) \
.option("header", True) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("year","race_year")

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, concat, col

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_renamed_df) \
                                          .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
                                          .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

races_final_df = races_with_timestamp_df.select(
    col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"), col("data_source")
)

# COMMAND ----------

# races_final_df.write.mode("overwrite").parquet("/mnt/formula1dlsc/processed/races")

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# %fs
# ls /mnt/formula1dlsc/processed/races

# COMMAND ----------

# df = spark.read.parquet("/mnt/formula1dlsc/processed/races")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Successful")
