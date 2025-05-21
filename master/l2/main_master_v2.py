# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # Master

# COMMAND ----------

from argparse import Namespace

from pyspark.sql.types import StructType
from pyspark.sql.window import Window 
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from util.jscs import JscsJob, get_start_end_time_for_master
from util.taskvalue import TaskValue
from util.deltatable import DeltaTableCRUD
from utils.utils import auto_fill_columns
from master.config import Config 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Job args

# COMMAND ----------

config = Config()

taskvalue = TaskValue()
workflow_time = taskvalue.get_args(taskKey=config.l2_taskName,
                                   key=config.taskKey,
                                   debugType="l2_l3")
n = Namespace(**workflow_time)

# COMMAND ----------

IS_TEST = False

if IS_TEST:
  START_TIME = ""
  END_TIME = ""
  JOB_NAME = f'F2C_H_DAX_FBK_MASTER'
  JOB_RUN = True
else:
  # START_TIME = ""
  # END_TIME = ""
  START_TIME = n.START_TIME
  END_TIME = n.END_TIME
  JOB_NAME = n.JOB_NAME

jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
jscs.get_job_dsp_dep()

if START_TIME == "" and  END_TIME == "":
  # jscs 取得撈取時間
  JOB_RUN = jscs.get_job_run()
  START_TIME, END_TIME = get_start_end_time_for_master(jscs, get_time=True)
  print(f"Start: {START_TIME}, End: {END_TIME}")
else:
  JOB_RUN = jscs.get_job_run(True)
  print(f"Start: {START_TIME}, End: {END_TIME}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Global Setup

# COMMAND ----------

STAGE_LIST = ['ARRAY', 'FEOL', 'BEOL']

# COMMAND ----------

# MAGIC %md
# MAGIC # Methods

# COMMAND ----------

def get_stage_temp_table(stage: str) -> str:
  table_list = {
    'ARRAY': config.mf_all_b_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp,
    'FEOL': config.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp,
    'BEOL':  config.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp
  }
  return table_list.get(stage)

def create_update_column(source_column, target_column, event_dtm):  
  return f"""
    CASE   
      WHEN (source.{event_dtm} >= target.{event_dtm} AND {target_column} IS NULL AND {source_column} IS NOT NULL)   
          THEN {source_column} 
      WHEN (source.{event_dtm} >= target.{event_dtm} AND {target_column} IS NOT NULL AND {source_column} IS NULL)  
          THEN {target_column}  
      ELSE {source_column} 
    END  
  """

def create_select_stage_column_backward(column):  
  return f"""
    CASE   
      WHEN A.{column} IS NOT NULL THEN A.{column}
      WHEN F.{column} IS NOT NULL THEN F.{column}
      WHEN B.{column} IS NOT NULL THEN B.{column}
      ELSE NULL
    END AS {column}
  """

def create_select_stage_column_forward(column):  
  return f"""
    CASE   
      WHEN B.{column} IS NOT NULL THEN B.{column}
      WHEN F.{column} IS NOT NULL THEN F.{column}
      WHEN A.{column} IS NOT NULL THEN A.{column}
      ELSE NULL
    END AS {column}
  """

def get_fab_with_fab_code(process_stage, fab_code):
  original_dict = config.fab_code_dict(process_stage)  
  reversed_dict = {value: key for key, value in original_dict.items()}  
  fab = reversed_dict.get(fab_code)  
  return fab

# COMMAND ----------

# MAGIC %md
# MAGIC # Select Cols

# COMMAND ----------

select_columns_backward = [
    # "CREATE_DATE", 
    # "CREATE_DTM",  
    "TFT_CHIP_ID_RRN",  
    "MAPPING_TFT_CHIP_ID_RRN",
    # "LENGTH_TFT_CHIP_ID_RRN",
    # "RANGE_TFT_CHIP_ID_RRN",
    "ARRAY_LOT_ID",  
    "ARRAY_SITE_ID",  
    "ARRAY_WO_ID",  
    "ARRAY_PRODUCT_CODE",  
    "ARRAY_ABBR_NO",  
    "ARRAY_MODEL_NO",  
    "ARRAY_PART_NO",  
    "ARRAY_1ST_GRADE",  
    "ARRAY_1ST_TEST_TIME",  
    "ARRAY_FINAL_GRADE",  
    "ARRAY_FINAL_TEST_TIME",  
    "ARRAY_SCRAP_FLAG",  
    "ARRAY_SCRAP_FLAG_DTM",  
    "ARRAY_SHIPPING_FLAG",  
    "ARRAY_SHIPPING_FLAG_DTM",  
    "ARRAY_START_DATE",  
    "ARRAY_CHIP_TYPE",  
    "ARRAY_FAB_CODE",  
    "ARRAY_WH_SHIPPING_DTM",  
    "ARRAY_COMP_DTM",  
    "ARRAY_INPUT_PART_NO",  
    "ARRAY_TERMINATE_FLAG",  
    "ARRAY_TERMINATE_FLAG_DTM",  
    "ARRAY_RECYCLE_FLAG",  
    "ARRAY_INPUT_CANCEL_FLAG",  
    "ARRAY_INPUT_CANCEL_FLAG_DTM",
    "ARRAY_UPDATE_DTM",  
    "CF_SITE_ID",  
    "CF_FAB_CODE",  
    "CF_LOT_ID",  
    "CF_GLASS_ID",  
    "CF_SUB_SHEET_ID",  
    "CF_CHIP_ID",  
    "CF_WO_ID",  
    "CF_PRODUCT_CODE",  
    "CF_ABBR_NO",
    "CF_MODEL_NO",  
    "CF_PART_NO",  
    "CF_FINAL_GRADE",  
    "CF_SHIPPING_FLAG",  
    "CF_SHIPPING_FLAG_DTM",
    "FEOL_ABBR_NO",  
    "FEOL_BATCH_ID",  
    "FEOL_CHIP_TYPE",  
    "FEOL_FAB_CODE",  
    "FEOL_INPUT_PART_NO",  
    "FEOL_MODEL_NO",  
    "FEOL_PART_NO",  
    "FEOL_PRODUCT_CODE",  
    "FEOL_RECEIVE_DTM",  
    "FEOL_SHIPPING_FLAG",  
    "FEOL_SHIPPING_FLAG_DTM",  
    "FEOL_SITE_ID",  
    "FEOL_START_DATE",  
    "FEOL_WH_RECEIVE_DTM",  
    "FEOL_WH_SHIPPING_DTM",  
    "FEOL_WO_ID",  
    "FEOL_UPDATE_DTM",  
    "BEOL_SITE_ID",  
    "BEOL_CHIP_TYPE",  
    "BEOL_WO_ID",  
    "BEOL_BATCH_ID",   
    "BEOL_PRODUCT_CODE",  
    "BEOL_ABBR_NO",  
    "BEOL_MODEL_NO",  
    "BEOL_PART_NO",  
    "BEOL_START_DATE",  
    "CELL_FINAL_GRADE",  
    "CELL_SCRAP_FLAG",  
    "CELL_SCRAP_FLAG_DTM",  
    "BEOL_SHIPPING_FLAG",  
    "BEOL_SHIPPING_FLAG_DTM",  
    "BEOL_RECEIVE_DTM",  
    "BEOL_FAB_CODE", 
    "BEOL_UPDATE_DTM",
    "PRT_FAB_CODE",  
    "PRT_WO_ID",  
    "PRT_PRODUCT_CODE",  
    "PRT_MODEL_NO",  
    "PRT_ABBR_NO",  
    "PRT_LOT_ID",  
    "PRT_GLASS_ID",  
    "PRT_CHIP_ID",  
    "PRT_PART_NO",  
    "PRT_FINAL_GRADE",  
    "PRT_SHIPPING_FLAG",  
    "PRT_SHIPPING_FLAG_DTM",  
    "CELL_PROCESS_TYPE",
    "CELL_PRODUCTION_AREA",    
    "CELL_SOURCE_FAB_CODE",  
    "CELL_INPUT_PART_NO", 
    # "CURRENT_STAGE",  
    # "EVENT_DTM",  
    "TFT_GLASS_ID",  
    "TFT_CHIP_ID",
    # "TFT_SUB_SHEET_ID",
    "UPDATE_DTM",  
]

select_columns_forward = [  
    "CURRENT_STAGE",
    "EVENT_DTM",
    "TFT_SUB_SHEET_ID"
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge Config

# COMMAND ----------

# DBTITLE 1,FLAG
def flag_merge_config():
  return {
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {  
        "ARRAY_UPDATE_FLAG": "source.ARRAY_UPDATE_FLAG",  
        "FEOL_UPDATE_FLAG": "source.FEOL_UPDATE_FLAG",  
        "BEOL_UPDATE_FLAG": "source.BEOL_UPDATE_FLAG"
    },
    'insert_cols': {
        "CREATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "TFT_CHIP_ID_RRN": "source.TFT_CHIP_ID_RRN",
        "ARRAY_UPDATE_FLAG": "source.ARRAY_UPDATE_FLAG",
        "FEOL_UPDATE_FLAG": "source.FEOL_UPDATE_FLAG",
        "BEOL_UPDATE_FLAG": "source.BEOL_UPDATE_FLAG"
    }
  }

# COMMAND ----------

# DBTITLE 1,ARRAY
def array_merge_config():
  return {
    # 'match_cols': ["ARRAY_UPDATE_FLAG", "FEOL_UPDATE_FLAG", "BEOL_UPDATE_FLAG", "TFT_CHIP_ID_RRN"],
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {  
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",  
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",  
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",  
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",  
        "ARRAY_LOT_ID": "CASE WHEN target.ARRAY_LOT_ID IS NULL THEN source.ARRAY_LOT_ID ELSE target.ARRAY_LOT_ID END",
        "TFT_GLASS_ID": "source.TFT_GLASS_ID",  
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",  
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",  
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",  
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",  
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",  
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",  
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",  
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",  
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",  
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",  
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",  
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",  
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",  
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",  
        "ARRAY_RECYCLE_FLAG_DTM": "source.ARRAY_RECYCLE_FLAG_DTM",  
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",  
        "ARRAY_INPUT_CANCEL_FLAG_DTM": "source.ARRAY_INPUT_CANCEL_FLAG_DTM",  
        "CURRENT_STAGE": """
            CASE 
              WHEN target.CURRENT_STAGE IS NULL THEN source.CURRENT_STAGE 
              WHEN source.EVENT_DTM >= target.EVENT_DTM THEN source.CURRENT_STAGE 
              ELSE target.CURRENT_STAGE 
            END""",  
        "EVENT_DTM": """
            CASE 
              WHEN target.EVENT_DTM IS NULL THEN source.EVENT_DTM 
              WHEN source.EVENT_DTM >= target.EVENT_DTM THEN source.EVENT_DTM 
              ELSE target.EVENT_DTM 
            END""",  
        "TFT_SUB_SHEET_ID": """
            CASE 
              WHEN target.TFT_SUB_SHEET_ID IS NULL OR target.TFT_SUB_SHEET_ID = '-' THEN source.TFT_SUB_SHEET_ID 
              ELSE target.TFT_SUB_SHEET_ID 
            END""",
        "ARRAY_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
    },
    'insert_cols': {
        "CREATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "TFT_CHIP_ID_RRN": "source.TFT_CHIP_ID_RRN",
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",
        "TFT_GLASS_ID": "source.TFT_GLASS_ID",
        "TFT_CHIP_ID": "source.TFT_CHIP_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_RECYCLE_FLAG_DTM": "source.ARRAY_RECYCLE_FLAG_DTM",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG_DTM": "source.ARRAY_INPUT_CANCEL_FLAG_DTM",
        "CURRENT_STAGE": "source.CURRENT_STAGE",
        "EVENT_DTM": "source.EVENT_DTM",
        "TFT_SUB_SHEET_ID": "source.TFT_SUB_SHEET_ID",
        "ARRAY_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
    }
  }

# COMMAND ----------

# DBTITLE 1,FEOL
def feol_merge_config():
  update_columns = ['FEOL_SITE_ID', 'FEOL_FAB_CODE', 'FEOL_CHIP_TYPE', 'FEOL_WO_ID', 'FEOL_BATCH_ID',   
                    'CF_SITE_ID', 'CF_FAB_CODE', 'CF_LOT_ID', 'CF_GLASS_ID', 'CF_SUB_SHEET_ID', 'CF_CHIP_ID',
                    'CF_WO_ID', 'CF_PRODUCT_CODE', 'CF_ABBR_NO', 'CF_MODEL_NO', 'CF_PART_NO', 'CF_FINAL_GRADE',
                    'CF_SHIPPING_FLAG', 'CF_SHIPPING_FLAG_DTM',
                    'FEOL_PRODUCT_CODE', 'FEOL_ABBR_NO', 'FEOL_MODEL_NO', 'FEOL_PART_NO',   
                    'CELL_PROCESS_TYPE', 'FEOL_SHIPPING_FLAG']
  return {
    # 'match_cols': ["ARRAY_UPDATE_FLAG", "FEOL_UPDATE_FLAG", "BEOL_UPDATE_FLAG", "TFT_CHIP_ID_RRN"],
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {
        **{col: create_update_column(f"source.{col}", f"target.{col}", "EVENT_DTM") for col in update_columns},   
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_1ST_GRADE": "source.ARRAY_1ST_GRADE",
        "ARRAY_1ST_TEST_TIME": "source.ARRAY_1ST_TEST_TIME",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_FINAL_TEST_TIME": "source.ARRAY_FINAL_TEST_TIME",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "ARRAY_WH_SHIPPING_DTM": "source.ARRAY_WH_SHIPPING_DTM",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_INPUT_PART_NO": "source.ARRAY_INPUT_PART_NO",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "TFT_SUB_SHEET_ID": """
          CASE 
            WHEN target.TFT_SUB_SHEET_ID IS NULL OR target.TFT_SUB_SHEET_ID = '-' THEN source.TFT_SUB_SHEET_ID 
            ELSE target.TFT_SUB_SHEET_ID 
          END""",
        "ARRAY_LOT_ID": """
          CASE 
            WHEN target.ARRAY_LOT_ID IS NULL OR target.ARRAY_LOT_ID = '-' THEN source.ARRAY_LOT_ID 
            ELSE target.ARRAY_LOT_ID 
          END""",
        "TFT_GLASS_ID": """
          CASE 
            WHEN target.TFT_GLASS_ID IS NULL OR target.TFT_GLASS_ID = '-' THEN source.TFT_GLASS_ID 
            ELSE target.TFT_GLASS_ID 
          END""",
        "CELL_SCRAP_FLAG": """
          CASE 
            WHEN (target.CELL_SCRAP_FLAG_DTM IS NULL AND source.CELL_SCRAP_FLAG_DTM IS NOT NULL) OR (source.CELL_SCRAP_FLAG_DTM > target.CELL_SCRAP_FLAG_DTM) 
            THEN source.CELL_SCRAP_FLAG
            ELSE target.CELL_SCRAP_FLAG
          END""",
        "CELL_SCRAP_FLAG_DTM": """
          CASE 
            WHEN (target.CELL_SCRAP_FLAG_DTM IS NULL AND source.CELL_SCRAP_FLAG_DTM IS NOT NULL) OR (source.CELL_SCRAP_FLAG_DTM > target.CELL_SCRAP_FLAG_DTM) 
            THEN source.CELL_SCRAP_FLAG_DTM
            ELSE target.CELL_SCRAP_FLAG_DTM
          END""",
        "CURRENT_STAGE": """
          CASE 
            WHEN target.CURRENT_STAGE is null THEN source.CURRENT_STAGE 
            WHEN source.EVENT_DTM >= target.EVENT_DTM THEN source.CURRENT_STAGE 
            ELSE target.CURRENT_STAGE 
          END""",  
        "EVENT_DTM": """
          CASE 
            WHEN target.EVENT_DTM is null THEN source.EVENT_DTM 
            WHEN source.EVENT_DTM >= target.EVENT_DTM THEN source.EVENT_DTM 
            ELSE target.EVENT_DTM 
          END""",
        "PRT_FAB_CODE": "source.PRT_FAB_CODE",  
        "PRT_WO_ID": "source.PRT_WO_ID",  
        "PRT_PRODUCT_CODE": "source.PRT_PRODUCT_CODE",  
        "PRT_MODEL_NO": "source.PRT_MODEL_NO",  
        "PRT_ABBR_NO": "source.PRT_ABBR_NO",  
        "PRT_LOT_ID": "source.PRT_LOT_ID",  
        "PRT_GLASS_ID": "source.PRT_GLASS_ID",  
        "PRT_CHIP_ID": "source.PRT_CHIP_ID",  
        "PRT_PART_NO": "source.PRT_PART_NO",  
        "PRT_FINAL_GRADE": "source.PRT_FINAL_GRADE",  
        "PRT_SHIPPING_FLAG": "source.PRT_SHIPPING_FLAG",  
        "PRT_SHIPPING_FLAG_DTM": "source.PRT_SHIPPING_FLAG_DTM",  
        "CELL_PRODUCTION_AREA": "source.CELL_PRODUCTION_AREA",  
        "CELL_SCRAP_FLAG_DTM": """
          CASE
            WHEN (target.CELL_SCRAP_FLAG_DTM IS NULL AND source.CELL_SCRAP_FLAG_DTM IS NOT NULL) OR (source.CELL_SCRAP_FLAG_DTM > target.CELL_SCRAP_FLAG_DTM) 
            THEN source.CELL_SCRAP_FLAG_DTM 
            ELSE target.CELL_SCRAP_FLAG_DTM 
          END""",   
        "FEOL_SHIPPING_FLAG_DTM": "source.FEOL_SHIPPING_FLAG_DTM",   
        "FEOL_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")
    },
    'insert_cols': {
        "CREATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),  
        "TFT_CHIP_ID_RRN": "source.TFT_CHIP_ID_RRN",  
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",  
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_1ST_GRADE": "source.ARRAY_1ST_GRADE",
        "ARRAY_1ST_TEST_TIME": "source.ARRAY_1ST_TEST_TIME",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_FINAL_TEST_TIME": "source.ARRAY_FINAL_TEST_TIME",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "ARRAY_WH_SHIPPING_DTM": "source.ARRAY_WH_SHIPPING_DTM",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_INPUT_PART_NO": "source.ARRAY_INPUT_PART_NO",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "FEOL_SITE_ID": "source.FEOL_SITE_ID",  
        "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "FEOL_CHIP_TYPE": "source.FEOL_CHIP_TYPE",  
        "FEOL_WO_ID": "source.FEOL_WO_ID",  
        "FEOL_BATCH_ID": "source.FEOL_BATCH_ID",  
        "TFT_GLASS_ID": "source.TFT_GLASS_ID",  
        "TFT_SUB_SHEET_ID": "source.TFT_SUB_SHEET_ID",  
        "TFT_CHIP_ID": "source.TFT_CHIP_ID",  
        "CF_SITE_ID": "source.CF_SITE_ID",  
        "CF_FAB_CODE": "source.CF_FAB_CODE",  
        "CF_LOT_ID": "source.CF_LOT_ID",  
        "CF_GLASS_ID": "source.CF_GLASS_ID",  
        "CF_SUB_SHEET_ID": "source.CF_SUB_SHEET_ID",  
        "CF_CHIP_ID": "source.CF_CHIP_ID",  
        "CF_WO_ID": "source.CF_WO_ID",  
        "CF_PRODUCT_CODE": "source.CF_PRODUCT_CODE",  
        "CF_ABBR_NO": "source.CF_ABBR_NO",  
        "CF_MODEL_NO": "source.CF_MODEL_NO",  
        "CF_PART_NO": "source.CF_PART_NO",  
        "CF_FINAL_GRADE": "source.CF_FINAL_GRADE",  
        "CF_SHIPPING_FLAG": "source.CF_SHIPPING_FLAG",  
        "CF_SHIPPING_FLAG_DTM": "source.CF_SHIPPING_FLAG_DTM",  
        "FEOL_PRODUCT_CODE": "source.FEOL_PRODUCT_CODE",  
        "FEOL_ABBR_NO": "source.FEOL_ABBR_NO",  
        "FEOL_MODEL_NO": "source.FEOL_MODEL_NO",  
        "FEOL_PART_NO": "source.FEOL_PART_NO",  
        "FEOL_START_DATE": "source.FEOL_START_DATE",  
        "CELL_PROCESS_TYPE": "source.CELL_PROCESS_TYPE",  
        "FEOL_SHIPPING_FLAG": "source.FEOL_SHIPPING_FLAG",  
        "FEOL_SHIPPING_FLAG_DTM": "source.FEOL_SHIPPING_FLAG_DTM",  
        "CELL_SCRAP_FLAG": "source.CELL_SCRAP_FLAG",  
        "CELL_SCRAP_FLAG_DTM": "source.CELL_SCRAP_FLAG_DTM",  
        "FEOL_RECEIVE_DTM": "source.FEOL_RECEIVE_DTM",  
        "CURRENT_STAGE": "source.CURRENT_STAGE",  
        "EVENT_DTM": "source.EVENT_DTM",  
        "PRT_FAB_CODE": "source.PRT_FAB_CODE",  
        "PRT_WO_ID": "source.PRT_WO_ID",  
        "PRT_PRODUCT_CODE": "source.PRT_PRODUCT_CODE",  
        "PRT_MODEL_NO": "source.PRT_MODEL_NO",  
        "PRT_ABBR_NO": "source.PRT_ABBR_NO",  
        "PRT_LOT_ID": "source.PRT_LOT_ID",  
        "PRT_GLASS_ID": "source.PRT_GLASS_ID",  
        "PRT_CHIP_ID": "source.PRT_CHIP_ID",  
        "PRT_PART_NO": "source.PRT_PART_NO",  
        "PRT_FINAL_GRADE": "source.PRT_FINAL_GRADE",  
        "PRT_SHIPPING_FLAG": "source.PRT_SHIPPING_FLAG",  
        "PRT_SHIPPING_FLAG_DTM": "source.PRT_SHIPPING_FLAG_DTM",  
        "CELL_PRODUCTION_AREA": "source.CELL_PRODUCTION_AREA",  
        "FEOL_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")
    }
  }

# COMMAND ----------

# DBTITLE 1,BEOL
def beol_merge_config():
  return {
    # 'match_cols': ["ARRAY_UPDATE_FLAG", "FEOL_UPDATE_FLAG", "BEOL_UPDATE_FLAG", "TFT_CHIP_ID_RRN"],
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",  
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_1ST_GRADE": "source.ARRAY_1ST_GRADE",
        "ARRAY_1ST_TEST_TIME": "source.ARRAY_1ST_TEST_TIME",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_FINAL_TEST_TIME": "source.ARRAY_FINAL_TEST_TIME",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "ARRAY_WH_SHIPPING_DTM": "source.ARRAY_WH_SHIPPING_DTM",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_INPUT_PART_NO": "source.ARRAY_INPUT_PART_NO",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "FEOL_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),  
        "FEOL_ABBR_NO": "source.FEOL_ABBR_NO",  
        "FEOL_BATCH_ID": "source.FEOL_BATCH_ID",  
        "FEOL_CHIP_TYPE": "source.FEOL_CHIP_TYPE",  
        "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "FEOL_INPUT_PART_NO": "source.FEOL_INPUT_PART_NO",  
        "FEOL_MODEL_NO": "source.FEOL_MODEL_NO",  
        "FEOL_PART_NO": "source.FEOL_PART_NO",  
        "FEOL_PRODUCT_CODE": "source.FEOL_PRODUCT_CODE",  
        "FEOL_RECEIVE_DTM": "source.FEOL_RECEIVE_DTM",  
        "FEOL_SHIPPING_FLAG": "source.FEOL_SHIPPING_FLAG",  
        "FEOL_SHIPPING_FLAG_DTM": "source.FEOL_SHIPPING_FLAG_DTM",  
        "FEOL_SITE_ID": "source.FEOL_SITE_ID",  
        "FEOL_START_DATE": "source.FEOL_START_DATE",  
        "FEOL_WH_RECEIVE_DTM": "source.FEOL_WH_RECEIVE_DTM",  
        "FEOL_WH_SHIPPING_DTM": "source.FEOL_WH_SHIPPING_DTM",  
        "FEOL_WO_ID": "source.FEOL_WO_ID",  
        "BEOL_SITE_ID": "source.BEOL_SITE_ID",      
        "BEOL_CHIP_TYPE": "source.BEOL_CHIP_TYPE",      
        "BEOL_WO_ID": "source.BEOL_WO_ID",      
        "BEOL_BATCH_ID": "source.BEOL_BATCH_ID",      
        "ARRAY_LOT_ID": """
          CASE 
            WHEN target.ARRAY_LOT_ID is null THEN source.ARRAY_LOT_ID
            ELSE target.ARRAY_LOT_ID
          END""",      
        "TFT_GLASS_ID": """
          CASE 
            WHEN target.TFT_GLASS_ID is null THEN source.TFT_GLASS_ID
            ELSE target.TFT_GLASS_ID
          END""",      
        "BEOL_PRODUCT_CODE": "source.BEOL_PRODUCT_CODE",      
        "BEOL_ABBR_NO": "source.BEOL_ABBR_NO",      
        "BEOL_MODEL_NO": "source.BEOL_MODEL_NO",      
        "BEOL_PART_NO": "source.BEOL_PART_NO",      
        "BEOL_START_DATE": "source.BEOL_START_DATE",      
        "CELL_FINAL_GRADE": """
          CASE 
            WHEN target.CELL_FINAL_GRADE is null THEN source.CELL_FINAL_GRADE
            ELSE target.CELL_FINAL_GRADE
          END""",      
        "CELL_SCRAP_FLAG": "source.CELL_SCRAP_FLAG",      
        "CELL_SCRAP_FLAG_DTM": "source.CELL_SCRAP_FLAG_DTM",      
        "BEOL_SHIPPING_FLAG": "source.BEOL_SHIPPING_FLAG",      
        "BEOL_SHIPPING_FLAG_DTM": "source.BEOL_SHIPPING_FLAG_DTM",      
        "GRADE_CHANGE_FLAG": """
          CASE   
            WHEN target.cell_final_grade is null OR target.module_final_grade is null THEN null  
            WHEN instr('ZP', substr(target.cell_final_grade, 1, 1)) > 0 AND instr('NVUS', substr(target.module_final_grade, 1, 1)) > 0 THEN 'DOWN'  
            WHEN instr('NVUS', substr(target.cell_final_grade, 1, 1)) > 0 AND instr('ZP', substr(target.module_final_grade, 1, 1)) > 0 THEN 'UP'  
            ELSE 'NONE'  
          END""",      
        "BEOL_RECEIVE_DTM": "source.BEOL_RECEIVE_DTM",      
        "BEOL_FAB_CODE": "source.BEOL_FAB_CODE",      
        "CURRENT_STAGE": """
          CASE 
            WHEN target.CURRENT_STAGE is null THEN source.CURRENT_STAGE
            WHEN source.EVENT_DTM > target.EVENT_DTM THEN source.CURRENT_STAGE
            ELSE target.CURRENT_STAGE
          END""",      
        "EVENT_DTM": """
          CASE 
            WHEN target.EVENT_DTM is null THEN source.EVENT_DTM
            WHEN source.EVENT_DTM > target.EVENT_DTM THEN source.EVENT_DTM
            ELSE target.EVENT_DTM
          END""",      
        "PRT_FAB_CODE": "source.PRT_FAB_CODE",      
        "PRT_WO_ID": "source.PRT_WO_ID",      
        "PRT_PRODUCT_CODE": "source.PRT_PRODUCT_CODE",      
        "PRT_MODEL_NO": "source.PRT_MODEL_NO",      
        "PRT_ABBR_NO": "source.PRT_ABBR_NO",      
        "PRT_LOT_ID": "source.PRT_LOT_ID",      
        "PRT_GLASS_ID": "source.PRT_GLASS_ID",      
        "PRT_CHIP_ID": "source.PRT_CHIP_ID",      
        "PRT_PART_NO": "source.PRT_PART_NO",      
        "PRT_FINAL_GRADE": "source.PRT_FINAL_GRADE",      
        "PRT_SHIPPING_FLAG": "source.PRT_SHIPPING_FLAG",      
        "PRT_SHIPPING_FLAG_DTM": "source.PRT_SHIPPING_FLAG_DTM",      
        "CELL_PRODUCTION_AREA": "source.CELL_PRODUCTION_AREA",      
        "TFT_SUB_SHEET_ID": """
          CASE 
            WHEN target.TFT_SUB_SHEET_ID is null or target.TFT_SUB_SHEET_ID = '-' THEN source.TFT_SUB_SHEET_ID
            ELSE target.TFT_SUB_SHEET_ID
          END""",      
        "CELL_SOURCE_FAB_CODE": """
          CASE 
            WHEN source.EVENT_DTM >= target.EVENT_DTM and target.BEOL_FAB_CODE is not null 
            THEN target.BEOL_FAB_CODE 
            ELSE target.CELL_SOURCE_FAB_CODE
          END""",      
        "CELL_INPUT_PART_NO": """
          CASE 
            WHEN source.EVENT_DTM >= target.EVENT_DTM and target.BEOL_PART_NO is not null 
            THEN target.BEOL_PART_NO 
            ELSE target.CELL_INPUT_PART_NO
          END""",      
        "BEOL_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")
    },
    'insert_cols': {
        "CREATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),  
        "TFT_CHIP_ID_RRN": "source.TFT_CHIP_ID_RRN",  
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",  
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_1ST_GRADE": "source.ARRAY_1ST_GRADE",
        "ARRAY_1ST_TEST_TIME": "source.ARRAY_1ST_TEST_TIME",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_FINAL_TEST_TIME": "source.ARRAY_FINAL_TEST_TIME",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "ARRAY_WH_SHIPPING_DTM": "source.ARRAY_WH_SHIPPING_DTM",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_INPUT_PART_NO": "source.ARRAY_INPUT_PART_NO",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),
        "FEOL_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"),  
        "FEOL_ABBR_NO": "source.FEOL_ABBR_NO",  
        "FEOL_BATCH_ID": "source.FEOL_BATCH_ID",  
        "FEOL_CHIP_TYPE": "source.FEOL_CHIP_TYPE",  
        "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "FEOL_INPUT_PART_NO": "source.FEOL_INPUT_PART_NO",  
        "FEOL_MODEL_NO": "source.FEOL_MODEL_NO",  
        "FEOL_PART_NO": "source.FEOL_PART_NO",  
        "FEOL_PRODUCT_CODE": "source.FEOL_PRODUCT_CODE",  
        "FEOL_RECEIVE_DTM": "source.FEOL_RECEIVE_DTM",  
        "FEOL_SHIPPING_FLAG": "source.FEOL_SHIPPING_FLAG",  
        "FEOL_SHIPPING_FLAG_DTM": "source.FEOL_SHIPPING_FLAG_DTM",  
        "FEOL_SITE_ID": "source.FEOL_SITE_ID",  
        "FEOL_START_DATE": "source.FEOL_START_DATE",  
        "FEOL_WH_RECEIVE_DTM": "source.FEOL_WH_RECEIVE_DTM",  
        "FEOL_WH_SHIPPING_DTM": "source.FEOL_WH_SHIPPING_DTM",  
        "FEOL_WO_ID": "source.FEOL_WO_ID",  
        "BEOL_SITE_ID": "source.BEOL_SITE_ID",  
        "BEOL_CHIP_TYPE": "source.BEOL_CHIP_TYPE",  
        "BEOL_WO_ID": "source.BEOL_WO_ID",  
        "BEOL_BATCH_ID": "source.BEOL_BATCH_ID",  
        "TFT_GLASS_ID": "source.TFT_GLASS_ID",  
        "TFT_CHIP_ID": "source.TFT_CHIP_ID",  
        "BEOL_PRODUCT_CODE": "source.BEOL_PRODUCT_CODE",  
        "BEOL_ABBR_NO": "source.BEOL_ABBR_NO",  
        "BEOL_MODEL_NO": "source.BEOL_MODEL_NO",  
        "BEOL_PART_NO": "source.BEOL_PART_NO",  
        "BEOL_START_DATE": "source.BEOL_START_DATE",  
        "CELL_FINAL_GRADE": "source.CELL_FINAL_GRADE",  
        "CELL_SCRAP_FLAG": "source.CELL_SCRAP_FLAG",  
        "CELL_SCRAP_FLAG_DTM": "source.CELL_SCRAP_FLAG_DTM",  
        "BEOL_SHIPPING_FLAG": "source.BEOL_SHIPPING_FLAG",  
        "BEOL_SHIPPING_FLAG_DTM": "source.BEOL_SHIPPING_FLAG_DTM",  
        "BEOL_RECEIVE_DTM": "source.BEOL_RECEIVE_DTM",  
        "BEOL_FAB_CODE": "source.BEOL_FAB_CODE",  
        "CURRENT_STAGE": "source.CURRENT_STAGE",  
        "EVENT_DTM": "source.EVENT_DTM",  
        "PRT_FAB_CODE": "source.PRT_FAB_CODE",  
        "PRT_WO_ID": "source.PRT_WO_ID",  
        "PRT_PRODUCT_CODE": "source.PRT_PRODUCT_CODE",  
        "PRT_MODEL_NO": "source.PRT_MODEL_NO",  
        "PRT_ABBR_NO": "source.PRT_ABBR_NO",  
        "PRT_LOT_ID": "source.PRT_LOT_ID",  
        "PRT_GLASS_ID": "source.PRT_GLASS_ID",  
        "PRT_CHIP_ID": "source.PRT_CHIP_ID",  
        "PRT_PART_NO": "source.PRT_PART_NO",  
        "PRT_FINAL_GRADE": "source.PRT_FINAL_GRADE",  
        "PRT_SHIPPING_FLAG": "source.PRT_SHIPPING_FLAG",  
        "PRT_SHIPPING_FLAG_DTM": "source.PRT_SHIPPING_FLAG_DTM",  
        "CELL_PRODUCTION_AREA": "source.CELL_PRODUCTION_AREA",  
        "TFT_SUB_SHEET_ID": "source.TFT_SUB_SHEET_ID",  
        "CELL_SOURCE_FAB_CODE": "source.CELL_SOURCE_FAB_CODE",  
        "CELL_INPUT_PART_NO": "source.CELL_INPUT_PART_NO",  
        "BEOL_UPDATE_DTM": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")
    }
  }

# COMMAND ----------

# DBTITLE 1,ALL STAGE
def all_stage_merge_config():
  update_columns = ['FEOL_SITE_ID', 'FEOL_FAB_CODE', 'FEOL_CHIP_TYPE', 'FEOL_WO_ID', 'FEOL_BATCH_ID',   
                  'CF_SITE_ID', 'CF_FAB_CODE', 'CF_LOT_ID', 'CF_GLASS_ID', 'CF_SUB_SHEET_ID', 'CF_CHIP_ID',
                  'CF_WO_ID', 'CF_PRODUCT_CODE', 'CF_ABBR_NO', 'CF_MODEL_NO', 'CF_PART_NO', 'CF_FINAL_GRADE',
                  'CF_SHIPPING_FLAG', 'CF_SHIPPING_FLAG_DTM',
                  'FEOL_PRODUCT_CODE', 'FEOL_ABBR_NO', 'FEOL_MODEL_NO', 'FEOL_PART_NO',   
                  'CELL_PROCESS_TYPE', 'FEOL_SHIPPING_FLAG']
  return {
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {
        **{col: create_update_column(f"source.{col}", f"target.{col}", "EVENT_DTM") for col in update_columns},   
        "ARRAY_LOT_ID": """
          CASE 
            WHEN target.ARRAY_LOT_ID IS NULL OR target.ARRAY_LOT_ID = '-' THEN source.ARRAY_LOT_ID 
            ELSE target.ARRAY_LOT_ID 
          END""", 
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_1ST_GRADE": "source.ARRAY_1ST_GRADE",
        "ARRAY_1ST_TEST_TIME": "source.ARRAY_1ST_TEST_TIME",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_FINAL_TEST_TIME": "source.ARRAY_FINAL_TEST_TIME",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_UPDATE_DTM": "source.ARRAY_UPDATE_DTM",
        "ARRAY_WH_SHIPPING_DTM": "source.ARRAY_WH_SHIPPING_DTM",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_INPUT_PART_NO": "source.ARRAY_INPUT_PART_NO",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG_DTM": "source.ARRAY_INPUT_CANCEL_FLAG_DTM",
        "UPDATE_DTM": "source.UPDATE_DTM",
        "FEOL_UPDATE_DTM": "source.FEOL_UPDATE_DTM",  
        # "FEOL_ABBR_NO": "source.FEOL_ABBR_NO",  
        # "FEOL_BATCH_ID": "source.FEOL_BATCH_ID",  
        # "FEOL_CHIP_TYPE": "source.FEOL_CHIP_TYPE",  
        # "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "FEOL_INPUT_PART_NO": "source.FEOL_INPUT_PART_NO",  
        # "FEOL_MODEL_NO": "source.FEOL_MODEL_NO",  
        # "FEOL_PART_NO": "source.FEOL_PART_NO",  
        # "FEOL_PRODUCT_CODE": "source.FEOL_PRODUCT_CODE",  
        "FEOL_RECEIVE_DTM": "source.FEOL_RECEIVE_DTM",  
        # "FEOL_SHIPPING_FLAG": "source.FEOL_SHIPPING_FLAG",  
        "FEOL_SHIPPING_FLAG_DTM": "source.FEOL_SHIPPING_FLAG_DTM",  
        # "FEOL_SITE_ID": "source.FEOL_SITE_ID",  
        "FEOL_START_DATE": "source.FEOL_START_DATE",  
        "FEOL_WH_RECEIVE_DTM": "source.FEOL_WH_RECEIVE_DTM",  
        "FEOL_WH_SHIPPING_DTM": "source.FEOL_WH_SHIPPING_DTM",  
        # "FEOL_WO_ID": "source.FEOL_WO_ID",  
        "BEOL_SITE_ID": "source.BEOL_SITE_ID",      
        "BEOL_CHIP_TYPE": "source.BEOL_CHIP_TYPE",      
        "BEOL_WO_ID": "source.BEOL_WO_ID",      
        "BEOL_BATCH_ID": "source.BEOL_BATCH_ID",      
        "ARRAY_LOT_ID": """
          CASE 
            WHEN target.ARRAY_LOT_ID is null THEN source.ARRAY_LOT_ID
            ELSE target.ARRAY_LOT_ID
          END""",      
        "TFT_GLASS_ID": """
          CASE 
            WHEN target.TFT_GLASS_ID IS NULL OR target.TFT_GLASS_ID = '-' THEN source.TFT_GLASS_ID 
            ELSE target.TFT_GLASS_ID 
          END""",     
        "BEOL_PRODUCT_CODE": "source.BEOL_PRODUCT_CODE",      
        "BEOL_ABBR_NO": "source.BEOL_ABBR_NO",      
        "BEOL_MODEL_NO": "source.BEOL_MODEL_NO",      
        "BEOL_PART_NO": "source.BEOL_PART_NO",      
        "BEOL_START_DATE": "source.BEOL_START_DATE",      
        "CELL_FINAL_GRADE": """
          CASE 
            WHEN target.CELL_FINAL_GRADE is null THEN source.CELL_FINAL_GRADE
            ELSE target.CELL_FINAL_GRADE
          END""",    
        "CELL_SCRAP_FLAG": "source.CELL_SCRAP_FLAG",
        "CELL_SCRAP_FLAG_DTM": "source.CELL_SCRAP_FLAG_DTM",                  
        # "CELL_SCRAP_FLAG": """
        #   CASE 
        #     WHEN (target.CELL_SCRAP_FLAG_DTM IS NULL AND source.CELL_SCRAP_FLAG_DTM IS NOT NULL) OR (source.CELL_SCRAP_FLAG_DTM > target.CELL_SCRAP_FLAG_DTM) 
        #     THEN source.CELL_SCRAP_FLAG
        #     ELSE target.CELL_SCRAP_FLAG
        #   END""",    
        #  "CELL_SCRAP_FLAG_DTM": """
        #   CASE 
        #     WHEN (target.CELL_SCRAP_FLAG_DTM IS NULL AND source.CELL_SCRAP_FLAG_DTM IS NOT NULL) OR (source.CELL_SCRAP_FLAG_DTM > target.CELL_SCRAP_FLAG_DTM) 
        #     THEN source.CELL_SCRAP_FLAG_DTM
        #     ELSE target.CELL_SCRAP_FLAG_DTM
        #   END""",      
        "BEOL_SHIPPING_FLAG": "source.BEOL_SHIPPING_FLAG",      
        "BEOL_SHIPPING_FLAG_DTM": "source.BEOL_SHIPPING_FLAG_DTM",      
        "GRADE_CHANGE_FLAG": """
          CASE   
            WHEN target.cell_final_grade is null OR target.module_final_grade is null THEN null  
            WHEN instr('ZP', substr(target.cell_final_grade, 1, 1)) > 0 AND instr('NVUS', substr(target.module_final_grade, 1, 1)) > 0 THEN 'DOWN'  
            WHEN instr('NVUS', substr(target.cell_final_grade, 1, 1)) > 0 AND instr('ZP', substr(target.module_final_grade, 1, 1)) > 0 THEN 'UP'  
            ELSE 'NONE'  
          END""",      
        "BEOL_RECEIVE_DTM": "source.BEOL_RECEIVE_DTM",      
        "BEOL_FAB_CODE": "source.BEOL_FAB_CODE",      
        "CURRENT_STAGE": """
          CASE 
            WHEN target.CURRENT_STAGE is null THEN source.CURRENT_STAGE
            WHEN source.EVENT_DTM >= target.EVENT_DTM THEN source.CURRENT_STAGE
            ELSE target.CURRENT_STAGE
          END""",      
        "EVENT_DTM": """
          CASE 
            WHEN target.EVENT_DTM is null THEN source.EVENT_DTM
            WHEN source.EVENT_DTM >= target.EVENT_DTM THEN source.EVENT_DTM
            ELSE target.EVENT_DTM
          END""",      
        "PRT_FAB_CODE": "source.PRT_FAB_CODE",      
        "PRT_WO_ID": "source.PRT_WO_ID",      
        "PRT_PRODUCT_CODE": "source.PRT_PRODUCT_CODE",      
        "PRT_MODEL_NO": "source.PRT_MODEL_NO",      
        "PRT_ABBR_NO": "source.PRT_ABBR_NO",      
        "PRT_LOT_ID": "source.PRT_LOT_ID",      
        "PRT_GLASS_ID": "source.PRT_GLASS_ID",      
        "PRT_CHIP_ID": "source.PRT_CHIP_ID",      
        "PRT_PART_NO": "source.PRT_PART_NO",      
        "PRT_FINAL_GRADE": "source.PRT_FINAL_GRADE",      
        "PRT_SHIPPING_FLAG": "source.PRT_SHIPPING_FLAG",      
        "PRT_SHIPPING_FLAG_DTM": "source.PRT_SHIPPING_FLAG_DTM",      
        "CELL_PRODUCTION_AREA": "source.CELL_PRODUCTION_AREA",      
        "TFT_SUB_SHEET_ID": """
          CASE 
            WHEN target.TFT_SUB_SHEET_ID is null or target.TFT_SUB_SHEET_ID = '-' THEN source.TFT_SUB_SHEET_ID
            ELSE target.TFT_SUB_SHEET_ID
          END""",      
        "CELL_SOURCE_FAB_CODE": """
          CASE 
            WHEN source.EVENT_DTM >= target.EVENT_DTM and target.BEOL_FAB_CODE is not null 
            THEN target.BEOL_FAB_CODE 
            ELSE target.CELL_SOURCE_FAB_CODE
          END""",      
        "CELL_INPUT_PART_NO": """
          CASE 
            WHEN source.EVENT_DTM >= target.EVENT_DTM and target.BEOL_PART_NO is not null 
            THEN target.BEOL_PART_NO 
            ELSE target.CELL_INPUT_PART_NO
          END""",      
        "BEOL_UPDATE_DTM": "source.BEOL_UPDATE_DTM",
        "UPDATE_DATE": "source.UPDATE_DATE",
        "MAPPING_TFT_CHIP_ID_RRN": "source.MAPPING_TFT_CHIP_ID_RRN",
    },
    'insert_cols': {
        "CREATE_DTM": "source.CREATE_DTM",  
        "TFT_CHIP_ID_RRN": "source.TFT_CHIP_ID_RRN", 
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",
        "ARRAY_LOT_ID": "source.ARRAY_LOT_ID",  
        "ARRAY_SITE_ID": "source.ARRAY_SITE_ID",
        "ARRAY_WO_ID": "source.ARRAY_WO_ID",
        "ARRAY_PRODUCT_CODE": "source.ARRAY_PRODUCT_CODE",
        "ARRAY_ABBR_NO": "source.ARRAY_ABBR_NO",
        "ARRAY_MODEL_NO": "source.ARRAY_MODEL_NO",
        "ARRAY_PART_NO": "source.ARRAY_PART_NO",
        "ARRAY_1ST_GRADE": "source.ARRAY_1ST_GRADE",
        "ARRAY_1ST_TEST_TIME": "source.ARRAY_1ST_TEST_TIME",
        "ARRAY_FINAL_GRADE": "source.ARRAY_FINAL_GRADE",
        "ARRAY_FINAL_TEST_TIME": "source.ARRAY_FINAL_TEST_TIME",
        "ARRAY_SCRAP_FLAG": "source.ARRAY_SCRAP_FLAG",
        "ARRAY_SCRAP_FLAG_DTM": "source.ARRAY_SCRAP_FLAG_DTM",
        "ARRAY_SHIPPING_FLAG": "source.ARRAY_SHIPPING_FLAG",
        "ARRAY_SHIPPING_FLAG_DTM": "source.ARRAY_SHIPPING_FLAG_DTM",
        "ARRAY_START_DATE": "source.ARRAY_START_DATE",
        "ARRAY_CHIP_TYPE": "source.ARRAY_CHIP_TYPE",
        "ARRAY_FAB_CODE": "source.ARRAY_FAB_CODE",
        "ARRAY_UPDATE_DTM": "source.ARRAY_UPDATE_DTM",
        "ARRAY_WH_SHIPPING_DTM": "source.ARRAY_WH_SHIPPING_DTM",
        "ARRAY_COMP_DTM": "source.ARRAY_COMP_DTM",
        "ARRAY_INPUT_PART_NO": "source.ARRAY_INPUT_PART_NO",
        "ARRAY_TERMINATE_FLAG": "source.ARRAY_TERMINATE_FLAG",
        "ARRAY_TERMINATE_FLAG_DTM": "source.ARRAY_TERMINATE_FLAG_DTM",
        "ARRAY_RECYCLE_FLAG": "source.ARRAY_RECYCLE_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG": "source.ARRAY_INPUT_CANCEL_FLAG",
        "ARRAY_INPUT_CANCEL_FLAG_DTM": "source.ARRAY_INPUT_CANCEL_FLAG_DTM",
        "FEOL_UPDATE_DTM": "source.FEOL_UPDATE_DTM",  
        "FEOL_ABBR_NO": "source.FEOL_ABBR_NO",  
        "FEOL_BATCH_ID": "source.FEOL_BATCH_ID",  
        "FEOL_CHIP_TYPE": "source.FEOL_CHIP_TYPE",  
        "FEOL_FAB_CODE": "source.FEOL_FAB_CODE",  
        "FEOL_INPUT_PART_NO": "source.FEOL_INPUT_PART_NO",  
        "FEOL_MODEL_NO": "source.FEOL_MODEL_NO",  
        "FEOL_PART_NO": "source.FEOL_PART_NO",  
        "FEOL_PRODUCT_CODE": "source.FEOL_PRODUCT_CODE",  
        "FEOL_RECEIVE_DTM": "source.FEOL_RECEIVE_DTM",  
        "FEOL_SHIPPING_FLAG": "source.FEOL_SHIPPING_FLAG",  
        "FEOL_SHIPPING_FLAG_DTM": "source.FEOL_SHIPPING_FLAG_DTM",  
        "FEOL_SITE_ID": "source.FEOL_SITE_ID",  
        "FEOL_START_DATE": "source.FEOL_START_DATE",  
        "FEOL_WH_RECEIVE_DTM": "source.FEOL_WH_RECEIVE_DTM",  
        "FEOL_WH_SHIPPING_DTM": "source.FEOL_WH_SHIPPING_DTM",  
        "FEOL_WO_ID": "source.FEOL_WO_ID",  
        "CF_SITE_ID": "source.CF_SITE_ID",  
        "CF_FAB_CODE": "source.CF_FAB_CODE",  
        "CF_LOT_ID": "source.CF_LOT_ID",  
        "CF_GLASS_ID": "source.CF_GLASS_ID",  
        "CF_SUB_SHEET_ID": "source.CF_SUB_SHEET_ID",  
        "CF_CHIP_ID": "source.CF_CHIP_ID",  
        "CF_WO_ID": "source.CF_WO_ID",  
        "CF_PRODUCT_CODE": "source.CF_PRODUCT_CODE",  
        "CF_ABBR_NO": "source.CF_ABBR_NO",  
        "CF_MODEL_NO": "source.CF_MODEL_NO",  
        "CF_PART_NO": "source.CF_PART_NO",  
        "CF_FINAL_GRADE": "source.CF_FINAL_GRADE",  
        "CF_SHIPPING_FLAG": "source.CF_SHIPPING_FLAG",  
        "CF_SHIPPING_FLAG_DTM": "source.CF_SHIPPING_FLAG_DTM",
        "BEOL_SITE_ID": "source.BEOL_SITE_ID",  
        "BEOL_CHIP_TYPE": "source.BEOL_CHIP_TYPE",  
        "BEOL_WO_ID": "source.BEOL_WO_ID",  
        "BEOL_BATCH_ID": "source.BEOL_BATCH_ID",  
        "BEOL_PRODUCT_CODE": "source.BEOL_PRODUCT_CODE",  
        "BEOL_ABBR_NO": "source.BEOL_ABBR_NO",  
        "BEOL_MODEL_NO": "source.BEOL_MODEL_NO",  
        "BEOL_PART_NO": "source.BEOL_PART_NO",  
        "BEOL_START_DATE": "source.BEOL_START_DATE",  
        "BEOL_SHIPPING_FLAG": "source.BEOL_SHIPPING_FLAG",  
        "BEOL_SHIPPING_FLAG_DTM": "source.BEOL_SHIPPING_FLAG_DTM",  
        "BEOL_RECEIVE_DTM": "source.BEOL_RECEIVE_DTM",  
        "BEOL_FAB_CODE": "source.BEOL_FAB_CODE",  
        "BEOL_UPDATE_DTM": "source.BEOL_UPDATE_DTM",
        "CELL_PRODUCTION_AREA": "source.CELL_PRODUCTION_AREA",  
        "CELL_SOURCE_FAB_CODE": "source.CELL_SOURCE_FAB_CODE",  
        "CELL_INPUT_PART_NO": "source.CELL_INPUT_PART_NO",  
        "CELL_FINAL_GRADE": "source.CELL_FINAL_GRADE",  
        "CELL_SCRAP_FLAG": "source.CELL_SCRAP_FLAG",  
        "CELL_SCRAP_FLAG_DTM": "source.CELL_SCRAP_FLAG_DTM",  
        "PRT_FAB_CODE": "source.PRT_FAB_CODE",  
        "PRT_WO_ID": "source.PRT_WO_ID",  
        "PRT_PRODUCT_CODE": "source.PRT_PRODUCT_CODE",  
        "PRT_MODEL_NO": "source.PRT_MODEL_NO",  
        "PRT_ABBR_NO": "source.PRT_ABBR_NO",  
        "PRT_LOT_ID": "source.PRT_LOT_ID",  
        "PRT_GLASS_ID": "source.PRT_GLASS_ID",  
        "PRT_CHIP_ID": "source.PRT_CHIP_ID",  
        "PRT_PART_NO": "source.PRT_PART_NO",  
        "PRT_FINAL_GRADE": "source.PRT_FINAL_GRADE",  
        "PRT_SHIPPING_FLAG": "source.PRT_SHIPPING_FLAG",  
        "PRT_SHIPPING_FLAG_DTM": "source.PRT_SHIPPING_FLAG_DTM",  
        "CURRENT_STAGE": "source.CURRENT_STAGE",  
        "EVENT_DTM": "source.EVENT_DTM",  
        "TFT_SUB_SHEET_ID": "source.TFT_SUB_SHEET_ID", 
        "TFT_GLASS_ID": "source.TFT_GLASS_ID",  
        "TFT_CHIP_ID": "source.TFT_CHIP_ID",  
        "UPDATE_DTM": "source.UPDATE_DTM",
        "UPDATE_DATE": "source.UPDATE_DATE",
        "MAPPING_TFT_CHIP_ID_RRN": "source.MAPPING_TFT_CHIP_ID_RRN",
    }
  }

# COMMAND ----------

# DBTITLE 1,create_time
def create_time_merge_config():
  return {
    'match_cols': ["TFT_CHIP_ID_RRN"],
    'update_cols': {
        "CREATE_DATE": "source.CREATE_DATE",  
        "CREATE_DTM": "source.CREATE_DTM",
        "LM_TIME": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")
    },
    'insert_cols': {
        "CREATE_DATE": "source.CREATE_DATE",  
        "CREATE_DTM": "source.CREATE_DTM",
        "LM_TIME": F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")
    }
  }

# COMMAND ----------

def get_merge_config(stage: str) -> str:
  merge_config_dict = {
    'ARRAY': array_merge_config(),
    'FEOL': feol_merge_config(),
    'BEOL': beol_merge_config(),
    'FLAG': flag_merge_config(),
    'ALL_STAGE': all_stage_merge_config(),
    'CREATE_TIME': create_time_merge_config()
  }
  return merge_config_dict.get(stage)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

def get_mapping_udate_time(df: SparkDataFrame, stage: str) -> str:
  if stage != 'ARRAY':  
    df = df.withColumn(  
        'ARRAY_UPDATE_DTM',   
        F.when(F.col('MAPPING_TFT_CHIP_ID_RRN') != F.col('TFT_CHIP_ID_RRN'),   
        F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")).otherwise(F.col('ARRAY_UPDATE_DTM'))  
    )  

  if stage == 'BEOL':  
      df = df.withColumn(  
          'FEOL_UPDATE_DTM',   
          F.when(F.col('MAPPING_TFT_CHIP_ID_RRN') != F.col('TFT_CHIP_ID_RRN'),   
          F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei")).otherwise(F.col('FEOL_UPDATE_DTM'))  
      )  

  return df  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # MASTER TABLE

# COMMAND ----------

if (JOB_RUN):
  try: 
      # if not IS_TEST: jscs.update_job_status("RUN")
      
      window = Window.partitionBy("TFT_CHIP_ID_RRN")
      # 取得各段 DF 
      stage_df_dict = {}
      for stage in STAGE_LIST:
        stage_df  = (
            spark.table(f'{config.delta_schema}.{get_stage_temp_table(stage)}')
            .withColumn('MAX_CREATE_DTM', F.max('CREATE_DTM').over(window))  
            .filter(F.col('CREATE_DTM') == F.col('MAX_CREATE_DTM'))
            .drop('MAX_CREATE_DTM')
            .distinct()
            .withColumn(f'{stage}_UPDATE_DTM', F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"))
            .transform(lambda x: get_mapping_udate_time(x, stage))
        )
        stage_df_dict[stage] = stage_df

      df = (
        auto_fill_columns(stage_df_dict['ARRAY'], f"{config.delta_schema}.{config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods}").alias('A')
        .join(auto_fill_columns(stage_df_dict['FEOL'], f"{config.delta_schema}.{config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods}").alias('F'), 
              on=["TFT_CHIP_ID_RRN"], how='full')
        .join(auto_fill_columns(stage_df_dict['BEOL'], f"{config.delta_schema}.{config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods}").alias('B'), 
              on=["TFT_CHIP_ID_RRN"], how='full')
        .selectExpr(
          *[create_select_stage_column_backward(col) for col in select_columns_backward],
          *[create_select_stage_column_forward(col) for col in select_columns_forward],
        )
        .withColumn('UPDATE_DTM', F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"))
        .withColumn('CREATE_DTM', F.from_utc_timestamp(F.current_timestamp(), "Asia/Taipei"))
        .withColumn('UPDATE_DATE', F.to_date(F.col('UPDATE_DTM')))
      )
      df.cache()
      df.display()

      merge_config = get_merge_config('ALL_STAGE')
      dt_crud = DeltaTableCRUD(f"{config.save_path}/{config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods}")
      dt_crud.upsert(
          update_delta_table=df,
          match_cols=merge_config.get('match_cols'),
          update_cols=merge_config.get('update_cols'),
          insert_cols=merge_config.get('insert_cols')
      )
      df.unpersist()

      # merge_config = get_merge_config('CREATE_TIME')
      # dt_crud = DeltaTableCRUD(f"{config.save_path}/{config.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time}")
      # dt_crud.upsert(
      #     update_delta_table=df,
      #     match_cols=merge_config.get('match_cols'),
      #     update_cols=merge_config.get('update_cols'),
      #     insert_cols=merge_config.get('insert_cols')
      # )


      if not IS_TEST: jscs.update_job_status("COMP")
  except Exception as e:  
      error_message = f"Error in stage: {stage}, Error: {e}." 
      if not IS_TEST: jscs.update_job_status("FAIL", error_message)
      raise  
else:
  print(f"Skip this cell run because the JOB_RUN is {JOB_RUN}.")
