# Databricks notebook source
from test.config import Config

# COMMAND ----------

config = Config()
jscs_db_schema = config.jscs_db_schema

print(jscs_db_schema)

# COMMAND ----------

# DBTITLE 1,ARRAY
PROCESS_STAGE = 'ARRAY'
for fab in ['L3C','L3D','L4A','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L7B','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,CELL
PROCESS_STAGE = 'CELL'
for fab in ['L3C','L3D','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L8A','L8B']:
  sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,MODULE
# PROCESS_STAGE = 'MODULE'
# for fab in ['M02', 'M11', 'S01', 'K01', 'S02', 'S03', 'S11', 'S06', 'K06', 'L6K', 'S17', 'L6B', 'L8B', 'L6K']:
#   sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
#   print(sql)
#   spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,BRISA
# PROCESS_STAGE = 'BRISA'
# for fab in ['S13','X13','Z0D','Z0H','Z0M','Z0W','Z1E','Z1F','Z1L','Z22','Z31','Z40','Z45','Z94','ZF2','ZK7','ZKC']:
#   sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
#   print(sql)
#   spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,LCM_BEOL
# PROCESS_STAGE = 'LCM_BEOL'
# for fab in ['S01', 'S02']:
#   sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
#   print(sql)
#   spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,EDI
# PROCESS_STAGE = 'EDI'
# for fab in ['F20','G23','G25','G30','V05','Z0S','Z19','Z1D','Z1F','Z1L','Z1S','Z1T','Z83','ZB1']:
#   sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
#   print(sql)
#   spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,EDI BEOL
# PROCESS_STAGE = 'EDI_BEOL'
# for fab in ['F20','Z0S']:
#   sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{fab}_{PROCESS_STAGE}_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
#   print(sql)
#   spark.sql(sql)

# COMMAND ----------

sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('LCM_F2C_H_DAX_FBK_TEST', '0,30 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
print(sql)
spark.sql(sql)
