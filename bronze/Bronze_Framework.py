# Databricks notebook source
# MAGIC %run ../lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 : Define Class**

# COMMAND ----------

@dataclass 
class BronzeLayer :
    pipeline_name:str
    delimiter:str
    header:bool
    raw_path:str
    bronze_table:str

    def __post_init__(self):
        print(f'{self.pipeline_name} pipeline')
        self.format_type = self.raw_path.split('.')[-1]
    
    def read_raw_data(self) -> DataFrame:
        return(
            spark.read.format(self.format_type)
            .option("delimiter", self.delimiter)
            .option("header", self.header)
            .load(self.raw_path)
            .withColumn('_load_dt',current_date())
            .withColumn('_load_dttm',current_timestamp())
            .withColumn('_file_name',col('_metadata.file_name'))
            .withColumn('_file_path',col('_metadata.file_path'))
            .withColumn('_file_size',col('_metadata.file_size'))
            .withColumn("_file_mod",col("_metadata.file_modification_time"))
        )
        
    
    def save_bronze(self, df:DataFrame) -> None:
         (
            df
            .write
            .mode("overwrite")
            .saveAsTable(self.bronze_table)
        )
         print('Save data to ',self.bronze_table,' success')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 : Instance Object**

# COMMAND ----------

data = BronzeLayer(pipeline_name= 'shop_table',
    delimiter="|",
    header=True,
    raw_path="/Volumes/workspace/project/manual_file_project/shop_mock.csv",
    bronze_table="project.shop_table_bronze_framework"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 : Read Raw Function**

# COMMAND ----------

bronze_df = data.read_raw_data()

# COMMAND ----------

# MAGIC %md
# MAGIC **step 3 : Load Data To Bronze Table**

# COMMAND ----------

data.save_bronze(bronze_df)

# COMMAND ----------

spark.table('project.shop_table_bronze_framework').display()