# Databricks notebook source
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType, DateType, IntegerType, DecimalType

from test.config import Config

# COMMAND ----------

config = Config()
db_schema = config.delta_schema
save_path = config.save_path

print(db_schema)
print(save_path)

# COMMAND ----------

target_delta_table_location = f"{save_path}/mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp"
tablename = f"{db_schema}.mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp"

df = spark.table(f"auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods")
target_schema = StructType(df.schema.fields)
empty_target_df = spark.createDataFrame(data=[], schema=target_schema)

empty_target_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename, path=f"{target_delta_table_location}")

# COMMAND ----------

spark.sql(f'ALTER TABLE {db_schema}.mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)')
spark.sql(f'ALTER TABLE {db_schema}.mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp CLUSTER BY (PROCESS_STAGE, FAB_CODE)') 
# OPTIMIZE auofabdev.prod_dw.mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp  
