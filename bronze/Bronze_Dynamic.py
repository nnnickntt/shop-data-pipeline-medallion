# Databricks notebook source
# MAGIC %run ../lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 : Config**

# COMMAND ----------

delimiter = '|'
header = True
format_type='csv'
write_mode = 'overwrite'
raw_path= '/Volumes/workspace/project/manual_file_project/shop_mock.csv'
bronze_table = 'project.shop_table_bronze_dynamic'

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 : Run Pipeline**

# COMMAND ----------

raw_data =(
    spark.read.format(format_type)
    .option('delimiter',delimiter)
    .option('header',header)
    .load(raw_path)
)

# COMMAND ----------

bronze_df = (
    raw_data.withColumn('_load_dt',current_date())
    .withColumn('_load_dttm',current_timestamp())
    .withColumn('_file_name',col('_metadata.file_name'))
    .withColumn('_file_path',col('_metadata.file_path'))
    .withColumn('_file_size',col('_metadata.file_size'))
    .withColumn("_file_mod",col("_metadata.file_modification_time")
    )
)

# COMMAND ----------

bronze_df.write.mode(write_mode).saveAsTable(bronze_table)

# COMMAND ----------

spark.table(bronze_table).limit(5).display()