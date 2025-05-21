# Databricks notebook source
from fma.config import Config

# COMMAND ----------

config = Config()
jscs_db_schema = config.jscs_db_schema

print(jscs_db_schema)

# COMMAND ----------

# DBTITLE 1,BRISAMODULE
PROCESS_STAGE = 'BRISAMODULE'
for fab in ['S13','X13','Z0D','Z0H','Z0M','Z0W','Z1F','Z1L','Z22','Z31','Z40','Z45','Z94','ZF2','ZK7','ZKC','Z1E']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_FMA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'CELL'
for fab in ['L3C','L3D','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_FMA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'LCMBEOL'
for fab in ['S01','S02']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_FMA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'MODULE'
for fab in ['K01','K06','L6K','M02','M11','S01','S02','S06','S11','S17']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_FMA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)
