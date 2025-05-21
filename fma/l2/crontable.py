# Databricks notebook source
# fab_list = ['L3C','L3D','L4A','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L7B','L8A','L8B'] # Fab ARRAY
fab_list = ['L3C','L5B','L5C','L5D','L6A','L6B','L6K','L7A','L8A','L8B'] #  Fab CELL
process_stage = "ARRAY" #ARRAY CELL 
table_name = "H_DAX_FBK_MATERIAL"

# SQL template
sql_template = 'INSERT INTO auofabdev.prod_dw.crontab  (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ("{Fab}_{ProcessStage}_F2C_{TableName}", "0,15,30,45 * * * *", "1", "Y","","","","","","","","","");'

# 產生insert sql
for fab in fab_list:
    sql_script = sql_template.format(Fab=fab, ProcessStage=process_stage, TableName=table_name)
    print(sql_script)

