# Databricks notebook source
from material.config import Config

# COMMAND ----------

config = Config()
jscs_db_schema = config.jscs_db_schema

print(jscs_db_schema)

# COMMAND ----------

fab_list = ['L3C','L3D','L4A','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L7B','L8A','L8B'] # Fab ARRAY
# fab_list = ['L3C','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L8A','L8B'] #  Fab CELL
process_stage = "ARRAY" #ARRAY CELL 
table_name = "H_DAX_FBK_MATERIAL"

# SQL template
sql_template = 'INSERT INTO auofabdev.prod_dw.crontab  (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ("{Fab}_{ProcessStage}_F2C_{TableName}", "0,15,30,45 * * * *", "1", "Y","","","","","","","","","");'

# 產生insert sql
for fab in fab_list:
    sql_script = sql_template.format(Fab=fab, ProcessStage=process_stage, TableName=table_name)
    print(sql_script)


# COMMAND ----------

PROCESS_STAGE = 'ARRAY'
for fab in ['L3C','L3D','L4A','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L7B','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_MATERIAL', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'CELL'
for fab in ['L3C','L3D','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_MATERIAL', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'MODULE'
for fab in ['K01', 'K06', 'L6K', 'L8B', 'M02', 'M11', 'S01', 'S02', 'S06', 'S11']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_MATERIAL', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'EDI'
for fab in ['Z19', 'Z1F', 'G23', 'G25', 'Z1L', 'Z1S', 'Z1T', 'G30', 'Z83', 'Z1D', 'ZB1']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_MATERIAL', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'BRISA'
for fab in ['X13', 'Z0M', 'Z45', 'S13', 'Z1E', 'Z31', 'Z40', 'ZK7', 'Z0W', 'Z0H', 'Z1F', 'Z0D', 'Z1L', 'ZF2', 'ZKC', 'Z22', 'Z94']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_MATERIAL', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

PROCESS_STAGE = 'LCMBEOL'
for fab in ['S01', 'S02']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_MATERIAL', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)
