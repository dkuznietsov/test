# Databricks notebook source
from scrap.config import Config

# COMMAND ----------

config = Config()
delta_schema = config.delta_schema
save_path = config.save_path

print(delta_schema)
print(save_path)

# COMMAND ----------

df = (
  spark.read.table("auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_scrap_ods")
  .withColumnRenamed("AZ_DB_TIME", "AZ_LM_TIME")
)

df.write.format("delta").mode("overwrite").saveAsTable(f"auofab.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_scrap_ods", path=f"{save_path}/mf_all_s_all_dw_at_feedback_h_dax_fbk_scrap_ods")
# df.display()
