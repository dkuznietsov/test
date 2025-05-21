# Databricks notebook source
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType, DateType, IntegerType, DecimalType

from fma.config import Config

# COMMAND ----------

config = Config()
db_schema = config.delta_schema
save_path = config.save_path

print(db_schema)
print(save_path)

# COMMAND ----------

target_delta_table_location = f"{save_path}/mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods"
tablename = f"{db_schema}.mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods"

from pyspark.sql.functions import col

df = (
  spark.table(f"auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods")
  .withColumnRenamed('AZ_DB_TIME', 'AZ_LM_TIME')
)

columns = ['FAB_CODE', 'PROCESS_STAGE'] + [col for col in df.columns if col not in ['FAB_CODE', 'PROCESS_STAGE']]
df = df.select(columns)
# target_schema = StructType(df.schema.fields)
# empty_target_df = spark.createDataFrame(data=[], schema=target_schema)

# dbutils.fs.rm(target_delta_table_location, True)
spark.sql(f"DROP TABLE IF EXISTS {tablename}")

df.write.format("delta").mode('overwrite').saveAsTable(tablename, path=f"{target_delta_table_location}")

# COMMAND ----------

spark.sql(f'ALTER TABLE {db_schema}.mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)')
spark.sql(f'ALTER TABLE {db_schema}.mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods CLUSTER BY (PROCESS_STAGE, FAB_CODE)') 

# COMMAND ----------

target_delta_table_location = f"{save_path}/mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods"
tablename = f"{db_schema}.mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods"


df = (
  spark.table(f"auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods")
  .withColumnRenamed('AZ_DB_TIME', 'AZ_LM_TIME')
)

df.write.format("delta").mode('append').saveAsTable(tablename, path=f"{target_delta_table_location}")
