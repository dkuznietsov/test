# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact

# COMMAND ----------

from argparse import Namespace
from functools import reduce  

from pyspark.sql.window import Window 
from pyspark.sql import functions as F

from util.jscs import JscsJob, get_start_end_time_for_master
from util.taskvalue import TaskValue
from util.deltatable import DeltaTableCRUD
from master.config import Config 

# COMMAND ----------

# MAGIC %md
# MAGIC # Job args

# COMMAND ----------

IS_TEST = True
config = Config()

db_schema = config.delta_schema
save_path = config.save_path

try:
  STAGE = dbutils.widgets.get('STAGE')
except:
  STAGE = 'BEOL'


def get_temp_job_args(taskKey):
  taskvalue = TaskValue()
  workflow_time = taskvalue.get_args(taskKey=taskKey,
                                    key=config.taskKey,
                                    debugType="l2_l3")
  n = Namespace(**workflow_time)
  return n

# COMMAND ----------

# MAGIC %md
# MAGIC # Methods

# COMMAND ----------

def get_stage_temp_table(stage: str) -> str:
  table_list = {
    'ARRAY': config.mf_all_s_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp,
    'FEOL': config.mf_all_s_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp,
    'BEOL':  config.mf_all_s_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp
  }
  return table_list.get(stage)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge Config

# COMMAND ----------

def get_merge_config():
  return {
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {  
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",  
        "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "BEOL_FAB_CODE": "source.BEOL_FAB_CODE",
        "LM_TIME": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
    },
    'insert_cols': {
        "CREATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "TFT_CHIP_ID_RRN": "source.TFT_CHIP_ID_RRN",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",  
        "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "BEOL_FAB_CODE": "source.BEOL_FAB_CODE",
        "LM_TIME": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
    }
  }

# COMMAND ----------

# MAGIC %md
# MAGIC # FACT TABLE

# COMMAND ----------

STAGE_LIST = ['ARRAY', 'FEOL', 'BEOL']

_df_dict = {}
for stage in STAGE_LIST:
  print(f"運行: {stage}")

  window = Window.partitionBy('TFT_CHIP_ID_RRN')
  # 取得各段 DF 
  stage_df  = (
    spark.table(f'{db_schema}.{get_stage_temp_table(stage)}')
    .select("TFT_CHIP_ID_RRN", f"{stage}_FAB_CODE", "CREATE_DTM")
    .withColumn('MAX_CREATE_DTM', F.max('CREATE_DTM').over(window))  
    .filter(F.col('CREATE_DTM') == F.col('MAX_CREATE_DTM'))
    .drop('MAX_CREATE_DTM', 'CREATE_DTM')
    .distinct()
  )
  _df_dict[stage] = stage_df
  actual_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), (_df_dict[key] for key in _df_dict.keys()))

actual_df.display()

merge_config = get_merge_config()
dt_crud = DeltaTableCRUD(f"{save_path}/{config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact}")
dt_crud.upsert(
    update_delta_table=actual_df,
    match_cols=merge_config.get('match_cols'),
    update_cols=merge_config.get('update_cols'),
    insert_cols=merge_config.get('insert_cols')
)
