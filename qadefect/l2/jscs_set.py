# Databricks notebook source
from qadefect.config import Config

# COMMAND ----------

config = Config()
jscs_db_schema = config.jscs_db_schema

print(jscs_db_schema)

# COMMAND ----------

PROCESS_STAGE = 'MODULE'
for fab in ['S01','S02','S06','L6K','S11']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QADEFECT', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)
