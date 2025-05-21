# Databricks notebook source
from datetime import datetime
from test.config import Config

# COMMAND ----------

config = Config()
db_schema = config.delta_schema
save_path = config.save_path

TABLE = dbutils.widgets.get('TABLE')

# COMMAND ----------

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
    execute_sql('OPTIMIZE', full_table_name, f'OPTIMIZE {full_table_name}')   
    return full_table_name  
  
def vacuum_table(full_table_name):
    """
    進行 vacuum_table
    :param full_table_name: 完整 tanle name  auofab.prod_dw.xxxxxx
    """  
    execute_sql('VACUUM', full_table_name, f'VACUUM {full_table_name}')   
    return full_table_name  
  
def delete_table(full_table_name):
    """
    進行 vacuum_table
    :param full_table_name: 完整 tanle name  auofab.prod_dw.xxxxxx
    """  
    execute_sql('DELETE', full_table_name, f'DELETE FROM {full_table_name}')   
    return full_table_name  

def _table(teble: str) -> str:
  table_list = {
    'MODULE': config.mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp,
    'TEST': config.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods
  }
  return table_list.get(teble)

# COMMAND ----------

full_table_name = f"{db_schema}.{_table(TABLE)}"
vacuum_table(full_table_name)
if TABLE == 'TEST':
  optimize_table(full_table_name)
if TABLE == 'MODULE':
  delete_table(full_table_name)
