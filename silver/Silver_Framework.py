# Databricks notebook source
# MAGIC %run ../lib

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step 0: Define Class and Functionality**
# MAGIC
# MAGIC - **Class:** `SilverLayer` (Cell 4)
# MAGIC   - Purpose: Handles reading, validating, and processing data from the bronze layer to the silver layer.
# MAGIC   - Key Methods:
# MAGIC     - `read_from_bronze_sk()`: Reads data from the bronze table and adds a unique key column (`_sk`).
# MAGIC     - `get_invalid_record()`: Identifies records with invalid data types or formats.
# MAGIC     - `get_null_record()`: Identifies records with null values in key columns.
# MAGIC     - `get_dup_record()`: Detects duplicate records based on all columns and key columns.
# MAGIC     - `get_all_bad_record()`: Aggregates all invalid, null, and duplicate records.
# MAGIC     - `get_all_good_record()`: Extracts valid records by excluding bad records.
# MAGIC     - `load_good_rec_to_table()`: Loads good records into the silver table.
# MAGIC     - `load_bad_rec_to_table()`: Loads bad records into the bad record table.
# MAGIC
# MAGIC - **Function:** `get_reason` (Cell 3)
# MAGIC   - Scope: **Global**
# MAGIC   - Purpose: Transforms a DataFrame to identify and aggregate the reasons (columns) for invalid, null, or duplicate records.
# MAGIC   - Usage: Utilized by `get_invalid_record()` and `get_null_record()` to summarize issues for each record.

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

@dataclass
class SilverLayer:
    pipeline_name:str
    schema_detail :dict[str,str]
    key:list[str]
    mode_type : str

    #Auto generate variable
    def __post_init__(self):
        self.bronze_table_name = f'project.{self.pipeline_name}_bronze_framework'
        self.silver_table_name = f'project.{self.pipeline_name}_silver_framework'
        self.bad_record_table_name= f'project.{self.pipeline_name}_bad_framework'
        self.data_column = list(self.schema_detail.keys())

    def read_from_bronze_sk(self)->DataFrame:
        return(
            spark.table(self.bronze_table_name)
            .select(
                monotonically_increasing_id().alias("_sk"),
                *self.data_column
                )
        )
    
    def get_invalid_record (self,bronze_df_framework:DataFrame)->DataFrame:
        invalid_rule = {
            'int' : '^[0-9]+$',
            'date' : '\\d{4}-\\d{2}-\\d{2}$'
        }
        invalid_column = {
            f'_is_{column_name}_invalid' : ~coalesce(col(column_name).rlike(invalid_rule[column_type]),lit(False))
            for column_name, column_type in self.schema_detail.items() if column_type != 'string'
        }       
        return (
            (   
                bronze_df_framework.withColumns(invalid_column)
                .transform(get_reason)
            )          
        )
    
    def get_null_record(self,bronze_df_framework:DataFrame)->DataFrame:
        null_column = {
            f'_is_{column_name}_null' : col(column_name).isNull() for column_name in self.key
        }
        return (
                bronze_df_framework.withColumns(null_column)
                .transform(get_reason)            
        )
    
    def get_dup_record(self,bronze_df_framework:DataFrame)->DataFrame:
        row_dup_partition = Window.partitionBy(*self.data_column).orderBy(asc('_sk'))
        key_dup_partition = Window.partitionBy('shop_id')

        bronze_df_not_null =(
            bronze_df_framework.join(self.get_null_record(bronze_df_framework),['_sk'],'left_anti')
        )

        row_dup_df =(
            bronze_df_not_null.withColumn('rn',row_number().over(row_dup_partition))
            .filter(col('rn')>1)
            .withColumn('reason',array(lit('_is_dup_row')))
            .drop('rn')      
        )

        key_dup_df = (
            bronze_df_not_null.join(row_dup_df,['_sk'],'left_anti')
            .withColumn('rn',count('*').over(key_dup_partition))
            .filter(col('rn')>1)
            .withColumn('reason',array(lit('_is_dup_key')))
            .drop('rn')
        )
        return (
            row_dup_df.union(key_dup_df)
        )
    
    def get_all_bad_record(self,invalid_df:DataFrame, null_df:DataFrame, dup_df:DataFrame)->DataFrame:
        return (
            invalid_df.unionByName(null_df).unionByName(dup_df)
            .groupBy('_sk', *self.data_column)
            .agg(flatten(collect_list('reason')).alias('reason'))
        )

    def get_all_good_record(self,bronze_df_framework:DataFrame,bad_rec_df:DataFrame)->DataFrame:
        return(
            bronze_df_framework.join(bad_rec_df,['_sk'],'left_anti')
        )
    
    def load_good_rec_to_table(self,good_rec_df:DataFrame)-> None:
        add_control_good_rec_df = (
            good_rec_df.select(*self.data_column)
            .withColumn('load_dt',current_date())
            .withColumn('load_dttm',current_timestamp()
            )
        )
        (
            add_control_good_rec_df
            .write
            .mode(self.mode_type)
            .saveAsTable(self.silver_table_name)
        )
        print(f'Load Good Data To {self.silver_table_name} success')
    
    def load_bad_rec_to_table(self,bad_rec_df:DataFrame)-> None:
        (
            bad_rec_df
            .write
            .mode(self.mode_type)
            .saveAsTable(self.bad_record_table_name)
        )
        print(f'Load Bad Data To {self.bad_record_table_name} success')


# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Start Pieline**
# MAGIC
# MAGIC - Instantiate `SilverLayer`
# MAGIC - Call methods to process data

# COMMAND ----------

test = SilverLayer(
    pipeline_name='shop_table',
    schema_detail = {
        'shop_id':'int',
        'shop_name':'string',
        'branch_name':'string',
        'file_dt':'date'
    },
    key=['shop_id'],
    mode_type='overwrite'
)

bronze_df = test.read_from_bronze_sk()
invalid_df = test.get_invalid_record(bronze_df_framework=bronze_df)
null_df = test.get_null_record(bronze_df_framework=bronze_df)
dup_df = test.get_dup_record(bronze_df_framework=bronze_df)
bad_rec_df = test.get_all_bad_record(invalid_df=invalid_df, null_df=null_df, dup_df=dup_df)
good_rec_df = test.get_all_good_record(bronze_df_framework = bronze_df,bad_rec_df= bad_rec_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Load Data To table**

# COMMAND ----------

test.load_bad_rec_to_table(bad_rec_df)
test.load_good_rec_to_table(good_rec_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4: Recheck Table**

# COMMAND ----------

spark.table('project.shop_table_silver_framework').limit(5).display()

# COMMAND ----------

spark.table('project.shop_table_bad_framework').limit(5).display()