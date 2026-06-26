# Databricks notebook source
# MAGIC %run ../lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 0 : Load Raw Data from Bronze Layer**
# MAGIC
# MAGIC - Add a surrogate key to uniquely identify each record. Surrogate keys help manage data integrity especially when raw keys are not unique or may change over time.

# COMMAND ----------

bronze_df = (
    spark.table('project.shop_table_bronze')
    .select(
        monotonically_increasing_id().alias('_sk'),
        'shop_id',
        'shop_name',
        'branch_name',
        'file_dt'
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1: Identify Invalid Rows (Result : final_invalid_df)**
# MAGIC
# MAGIC - `shop_id`: Should be an integer
# MAGIC - `file_dt`: Should be a valid date
# MAGIC - melt function : Use melt to pivot 

# COMMAND ----------

integer_format = "^[0-9]+$"
date_format = "\\d{4}-\\d{2}-\\d{2}$"

invalid_df = (
    bronze_df.withColumn('_is_shop_id_invalid',coalesce(~col('shop_id').rlike(integer_format),lit(False)))
    .withColumn('_is_file_dt_invalid',coalesce(~col('file_dt').rlike(date_format),lit(False)))
    .filter(col('_is_shop_id_invalid') | col('_is_file_dt_invalid'))
)

final_invalid_df =(invalid_df.melt(
        ids=['_sk','shop_id','shop_name','branch_name','file_dt'],
        values=['_is_shop_id_invalid','_is_file_dt_invalid'],
        variableColumnName='reason',
        valueColumnName='status'
     )
    .filter(col('status')==True)
    .groupby('_sk','shop_id','shop_name','branch_name','file_dt')
    .agg(
        collect_list(col('reason')).alias('reason')
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Identify Null Rows (Result :final_null_df)**
# MAGIC
# MAGIC - `shop_id`: Should not be null
# MAGIC - melt function : Use melt to pivot 

# COMMAND ----------

null_df =(
    bronze_df.withColumn('_is_shop_id_null',col("shop_id").isNull())
    .filter(col('_is_shop_id_null')==True)
)
final_null_df = (
    null_df.melt(
        ids=['_sk','shop_id','shop_name','branch_name','file_dt'],
        values=['_is_shop_id_null'],
        variableColumnName='reason',
        valueColumnName='status'
     )
    .groupby('_sk','shop_id','shop_name','branch_name','file_dt')
    .agg(
        collect_list(col('reason')).alias('reason')
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Identify Duplicate Row & Key (Result :final_dup_df)**
# MAGIC - Remove Null Keys First, Then Identify Duplicates
# MAGIC - Identify Row Dubplicate (row_dup_df)
# MAGIC - Identify Key Dubplicate (key_dup_df)
# MAGIC - Union row_dup_df and key_dup_df

# COMMAND ----------

bronze_df_not_null = (
    bronze_df.join(final_null_df,['_sk'],'left_anti')
)
row_dup_df =(
    bronze_df_not_null.withColumn(
        'rn',row_number().over(Window.partitionBy('shop_id','shop_name','branch_name','file_dt').orderBy('_sk'))
    )
    .filter(col('rn')>1)
    .drop('rn')
    .withColumn('reason',array(lit('_is_dup_row')))
)

# COMMAND ----------

key_dup_df=(
    bronze_df_not_null.join(row_dup_df,['_sk'],'left_anti')
    .withColumn(
        'rn',count('*').over(Window.partitionBy('shop_id'))
    )
    .filter(col('rn')>1)
    .drop('rn')
    .withColumn('reason',array(lit('_is_dup_key')))
)

# COMMAND ----------

final_dup_df = row_dup_df.unionByName(key_dup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4: Combine All Bad Records (Result: all_bad_record_df)**
# MAGIC
# MAGIC - Union duplicate, invalid, and null records into a single DataFrame.
# MAGIC

# COMMAND ----------

all_bad_record_df =(
    final_dup_df.unionByName(final_invalid_df).unionByName(final_null_df)
    .groupBy("_sk","shop_id","shop_name","branch_name","file_dt")
    .agg(
        flatten(collect_list(col('reason'))).alias('reason')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5: Keep Good Records**
# MAGIC
# MAGIC - Use a left anti join between the bronze DataFrame and all_bad_record_df to retain only valid records.

# COMMAND ----------

all_good_record_df = (
    bronze_df.join(all_bad_record_df,['_sk'],how='left_anti')
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6: Load Data**
# MAGIC
# MAGIC - Write **valid records** to the Silver table, ensuring correct data types and adding control columns (`load_dt`, `load_dttm`).
# MAGIC - Store **invalid records** in the Bad table for further analysis.

# COMMAND ----------

final_good_record =(
    all_good_record_df.select(
        col("shop_id").cast('int'),
        col("shop_name").cast('string'),
        col("branch_name").cast('string'),
        col("file_dt").cast('date'),
        current_date().alias('load_dt'),
        current_timestamp().alias('load_dttm')
        )
)

# COMMAND ----------

final_good_record.write.mode('overwrite').saveAsTable('project.shop_table_silver')
all_bad_record_df.write.mode('overwrite').saveAsTable('project.shop_table_bad')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7: Check result**

# COMMAND ----------

good_result = spark.table('project.shop_table_silver')
good_result.display()

# COMMAND ----------

bad_result = spark.table('project.shop_table_bad')
bad_result.display()