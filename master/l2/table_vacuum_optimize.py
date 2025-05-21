# Databricks notebook source
from datetime import datetime
from master.config import Config

# COMMAND ----------

config = Config()
db_schema = config.delta_schema
save_path = config.save_path

TABLE = dbutils.widgets.get('TABLE')

# COMMAND ----------

# DBTITLE 1,Methods
def execute_sql(operation: str, full_table_name: str, sql: str):
    """
    執行SQL
    :param operation: 執行哪一種SQL  ALTER OPTIMIZE/OPTIMIZE/VACUUM
    :param full_table_name: 完整 tanle name  auofab.prod_dw.xxxxxx
    :param sql: 執行的SQL
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
    print(f'{operation} {full_table_name}, Start Time: {now}')  
    spark.sql(f"{sql}")
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  
    print(f'{operation} {full_table_name}, End Time: {end_time}')  

def optimize_table(full_table_name):
    """
    進行 optimize_table
    :param full_table_name: 完整 tanle name  auofab.prod_dw.xxxxxx
    """
    # print(f'OPTIMIZE {full_table_name}, Start Time: {now}')
    # spark.sql(f'OPTIMIZE {full_table_name}') 
    execute_sql('OPTIMIZE', full_table_name, f'OPTIMIZE {full_table_name}')   
    return full_table_name  

def vacuum_table(full_table_name):
    """
    進行 vacuum_table
    :param full_table_name: 完整 tanle name  auofab.prod_dw.xxxxxx
    """
    # print(f'OPTIMIZE {full_table_name}, Start Time: {now}')
    # spark.sql(f'VACUUM {full_table_name}')  
    execute_sql('VACUUM', full_table_name, f'VACUUM {full_table_name}')   
    return full_table_name  

def _table(teble: str) -> str:
  table_list = {
    'ARRAY': config.mf_all_b_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp,
    'FEOL': config.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp,
    'BEOL': config.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp,
    'MASTER': config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods
  }
  return table_list.get(teble)

# COMMAND ----------

# DBTITLE 1,MAIN
full_table_name = f"{db_schema}.{_table(TABLE)}"
vacuum_table(full_table_name)
if TABLE == 'MASTER':
  optimize_table(full_table_name)
