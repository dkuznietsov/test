# Databricks notebook source
from qa.config import Config

# COMMAND ----------

config = Config()
jscs_db_schema = config.jscs_db_schema

print(jscs_db_schema)

# COMMAND ----------

PROCESS_STAGE = 'ARRAY'
for fab in ['L3C','L3D','L4A','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L7B','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'CELL'
for fab in ['L3C','L3D','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'EDIBEOL'
for fab in ['F20']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'EDIMODULE'
for fab in ['ZB1','Z83','Z1T','Z1S','Z1L','Z1F','Z1D','Z19','Z0S','G30','G25','G23','F20']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'BRISAMODULE'
for fab in ['S13','X13','Z0D','Z0H','Z0M','Z0W','Z1E','Z1F','Z1L','Z22','Z31','Z40','Z45','Z94','ZF2','ZK7','ZKC']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'MODULE'
for fab in ['K01','K06','L6K','M02','M11','S01','S02','S06','S11','S17','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_QA', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)
