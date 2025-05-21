# Databricks notebook source
from master.config import Config
from utils.utils import auto_fill_columns

# COMMAND ----------

config = Config()

SAVE_SCHEMA = config.delta_schema
SAVE_PATH = config.save_path

print(f"SAVE_SCHEMA: {SAVE_SCHEMA}")
print(f"SAVE_PATH: {SAVE_PATH}")

# COMMAND ----------

MASTER_TABLE = 'mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods'

# COMMAND ----------

df = (
  spark.table("auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_master_ods")
  .transform(auto_fill_columns, f"{SAVE_SCHEMA}.{MASTER_TABLE}")
)
df.display()

# COMMAND ----------

df.write.format("delta") \
  .mode('overwrite') \
  .saveAsTable(f"{SAVE_SCHEMA}.{MASTER_TABLE}", path=f"{SAVE_PATH}/{MASTER_TABLE}")
