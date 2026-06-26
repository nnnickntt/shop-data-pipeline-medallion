# Databricks notebook source
# MAGIC %run ../lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 0 : Configure Data (Schema, Table Columns, Table Names, Bronze Table, and Melt Function)**

# COMMAND ----------

bronze_table_name='project.shop_table_bronze_dynamic'
silver_table_name='project.shop_table_silver_dynamic'
bad_record_table_name='project.shop_table_bad_dynamic'

#Schema Column
schema_detail ={
    'shop_id':'int',
    'shop_name':'string',
    'branch_name':'string',
    'file_dt':'date'
}

#Data Column
data_column = [column for column in schema_detail.keys()]

# COMMAND ----------

bronze_df = (
    spark.table(bronze_table_name)
    .select(
        monotonically_increasing_id().alias("_sk")
        ,*schema_detail.keys()
    )
)

# COMMAND ----------

def get_reason(df:DataFrame)->DataFrame:

    value_column =[column for column in df.columns if column.startswith('_')and column != '_sk']
    id_column = [column for column in df.columns if not column.startswith('_') or column == '_sk']
    filter_column = " or ".join(value_column)

    #print(value_column)
    #print(id_column)
    
    return(
        df.
        filter(filter_column)
        .melt(
        ids=id_column,
        values=value_column,
        variableColumnName='reason',
        valueColumnName='status'
     )
        .filter(col('status')==True)
        .orderBy('_sk')
        .groupby(id_column)
        .agg(
            collect_list(col('reason')).alias('reason')
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 : Identify Invalid Rows (Result : final_invalid_df)**

# COMMAND ----------

#Config Rule
invalid_rule = {
    'int' : '^[0-9]+$',
    'date' : '\\d{4}-\\d{2}-\\d{2}$'
}

#Extract Invalid Column
invalid_column = {
    f'_is_{column_name}_invalid' : ~coalesce(col(column_name).rlike(invalid_rule[column_type]),lit(False)) for column_name, column_type in schema_detail.items() if column_type != 'string'
}

# COMMAND ----------

final_invalid_df = (
    bronze_df.withColumns(invalid_column)
    .transform(get_reason)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 : Identify Null Rows (Result :final_null_df)**
# MAGIC
# MAGIC - `shop_id`: Should not be null

# COMMAND ----------

config_key=['shop_id']
null_column = {
    f'_is_{column_name}_null' : col(column_name).isNull() for column_name in config_key
}

# COMMAND ----------

final_null_df = (
    bronze_df.withColumns(null_column)
    .transform(get_reason)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Identify Duplicate Row & Key (Result :final_dup_df)**
# MAGIC - Remove Null Keys First, Then Identify Duplicates
# MAGIC - Identify Row Duplicate (row_dup_df)
# MAGIC - Identify Key Duplicate (key_dup_df)
# MAGIC - Union row_dup_df and key_dup_df

# COMMAND ----------

bronze_df_not_null = (
    bronze_df.join(final_null_df,['_sk'],'left_anti')
)

row_dup_partition = Window.partitionBy(data_column).orderBy(asc('_sk'))

row_dup_df =(
    bronze_df_not_null.withColumn('rn',row_number().over(row_dup_partition))
    .filter(col('rn')>1)
    .withColumn('_is_dup_row',lit(True))
    .drop('rn')
    .transform(get_reason)
)

# COMMAND ----------

key_dup_partition = Window.partitionBy('shop_id')

key_dup_df = (
    bronze_df_not_null.join(row_dup_df,['_sk'],'left_anti')
    .withColumn('rn',count('*').over(key_dup_partition))
    .filter(col('rn')>1)
    .withColumn('_is_dup_key',lit(True))
    .drop('rn')
    .transform(get_reason)
)

# COMMAND ----------

final_dup_df  = row_dup_df.unionByName(key_dup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 : Combine All Bad Records (Result: all_bad_record_df)**
# MAGIC
# MAGIC - The `flatten` function is used to merge lists of reasons from different sources into a single list for each record

# COMMAND ----------

all_bad_record_df = (
    final_dup_df.unionByName(final_invalid_df).unionByName(final_null_df)
    .groupBy('_sk', *data_column)
    .agg(flatten(collect_list('reason')).alias('reason'))
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
# MAGIC %md
# MAGIC **Step 6: Load Data**
# MAGIC
# MAGIC - Write **valid records** to the Silver table, ensuring correct data types and adding control columns (`load_dt`, `load_dttm`).
# MAGIC - Store **invalid records** in the Bad table for further analysis.

# COMMAND ----------

casting_column ={
    column_name : col(column_name).cast(column_type) for column_name,column_type in schema_detail.items()
}

final_good_record_df = (
    all_good_record_df.select(
        *casting_column.values(),
        current_date().alias('load_dt'),
        current_timestamp().alias('load_dttm')
    )
)

# COMMAND ----------

final_good_record_df.write.mode('overwrite').saveAsTable(silver_table_name)
all_bad_record_df.write.mode('overwrite').saveAsTable(bad_record_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC **step 7: Check result**

# COMMAND ----------

spark.table(silver_table_name).display()

# COMMAND ----------

spark.table(bad_record_table_name).display()