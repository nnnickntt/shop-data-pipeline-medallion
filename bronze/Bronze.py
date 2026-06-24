# Databricks notebook source
# MAGIC %run ../lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 : Extract Raw Data**

# COMMAND ----------

raw_data =(
    spark.read.format('csv')
    .option('delimiter','|')
    .option('header',True)
    .load('/Volumes/workspace/project/manual_file_project/shop_mock.csv')
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Add Control Column and Save to Bronze Table**

# COMMAND ----------

bronze_df = (
    raw_data.withColumn('_load_dt',current_date())
    .withColumn('_load_dttm',current_timestamp())
    .withColumn('_file_name',col('_metadata.file_name'))
    .withColumn('_file_path',col('_metadata.file_path'))
    .withColumn('_file_size',col('_metadata.file_size'))
    .withColumn("_file_mod",col("_metadata.file_modification_time"))
)
#bronze_df.limit(5).display()

# COMMAND ----------

bronze_df.write.mode('overwrite').saveAsTable('project.shop_table_bronze')

# COMMAND ----------

spark.table('project.shop_table_bronze').limit(5).display()