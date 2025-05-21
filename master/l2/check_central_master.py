# Databricks notebook source
import pytz
import os
import unittest
from datetime import datetime, time, timedelta
from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F

# COMMAND ----------

def round_time(dt, round_to=15):  
    dt = dt.replace(tzinfo=None)  
    seconds_past_hour = (dt - dt.replace(minute=0, second=0, microsecond=0)).total_seconds()  
    rounding = (seconds_past_hour + round_to*30) // (round_to*60) * (round_to*60)  
    return dt + timedelta(seconds=rounding-seconds_past_hour)  

# COMMAND ----------

IS_TEST = False
TIME_ZONE = pytz.timezone('Asia/Taipei')

try:
  START_TIME = dbutils.widgets.get("CHECK_START_TIME")
  END_TIME = dbutils.widgets.get("CHECK_END_TIME")
  PROCESS_STAGE = dbutils.widgets.get("PROCESS_STAGE")
except:
  if IS_TEST:
    PROCESS_STAGE = 'ARRAY'
    
if START_TIME == '' and END_TIME == '':
  now = datetime.now(TIME_ZONE) - timedelta(days=1) 
  new_time = now - timedelta(minutes=15)

  rounded_now = round_time(now, 15)  
  rounded_new_time = round_time(new_time, 15)  
    
  START_TIME = rounded_new_time.strftime('%Y-%m-%d %H:%M:00')  
  END_TIME = rounded_now.strftime('%Y-%m-%d %H:%M:00')  

print(f"START_TIME = {START_TIME}")
print(f"END_TIME = {END_TIME}")

# COMMAND ----------

PK_COLS_LIST = ['TFT_CHIP_ID_RRN']

# COMMAND ----------

def get_check_cols():
    _dict = {
        "ARRAY": [
            "ARRAY_SITE_ID",
            "ARRAY_FAB_CODE",
            "ARRAY_CHIP_TYPE",
            "ARRAY_WO_ID",
            "ARRAY_LOT_ID",
            "TFT_GLASS_ID",
            "TFT_CHIP_ID",
            "ARRAY_PRODUCT_CODE",
            "ARRAY_ABBR_NO",
            "ARRAY_MODEL_NO",
            "ARRAY_PART_NO",
            "ARRAY_START_DATE",
            "ARRAY_FINAL_GRADE",
            "ARRAY_COMP_DTM",
            "ARRAY_SCRAP_FLAG",
            "ARRAY_SCRAP_FLAG_DTM",
            "ARRAY_SHIPPING_FLAG",
            "ARRAY_SHIPPING_FLAG_DTM",
            "ARRAY_TERMINATE_FLAG",
            "ARRAY_TERMINATE_FLAG_DTM",
            "ARRAY_RECYCLE_FLAG",
            "ARRAY_RECYCLE_FLAG_DTM",
            "ARRAY_INPUT_CANCEL_FLAG",
            "ARRAY_INPUT_CANCEL_FLAG_DTM",
            "TFT_SUB_SHEET_ID",
        ],  # 移除 TFT_CHIP_ID_RRN, CURRENT_STAGE
        "FEOL": [
            "ARRAY_LOT_ID",
            "ARRAY_SITE_ID",
            "ARRAY_FAB_CODE",
            "ARRAY_CHIP_TYPE",
            "ARRAY_WO_ID",
            "ARRAY_PRODUCT_CODE",
            "ARRAY_ABBR_NO",
            "ARRAY_MODEL_NO",
            "ARRAY_PART_NO",
            "ARRAY_START_DATE",
            "ARRAY_FINAL_GRADE",
            "ARRAY_FINAL_TEST_TIME",
            "ARRAY_COMP_DTM",
            "ARRAY_SCRAP_FLAG",
            "ARRAY_SCRAP_FLAG_DTM",
            "ARRAY_SHIPPING_FLAG",
            "ARRAY_SHIPPING_FLAG_DTM",
            "ARRAY_TERMINATE_FLAG",
            "ARRAY_TERMINATE_FLAG_DTM",
            "ARRAY_RECYCLE_FLAG",
            "ARRAY_RECYCLE_FLAG_DTM",
            "ARRAY_INPUT_CANCEL_FLAG",
            "ARRAY_INPUT_CANCEL_FLAG_DTM",
            "ARRAY_WH_SHIPPING_DTM",
            "ARRAY_INPUT_PART_NO",
            "FEOL_SITE_ID",
            "FEOL_FAB_CODE",
            "FEOL_CHIP_TYPE",
            "FEOL_WO_ID",
            "FEOL_BATCH_ID",
            "TFT_GLASS_ID",
            "TFT_SUB_SHEET_ID",
            "TFT_CHIP_ID",
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
            "FEOL_PRODUCT_CODE",
            "FEOL_ABBR_NO",
            "FEOL_MODEL_NO",
            "FEOL_PART_NO",
            "FEOL_START_DATE",
            "CELL_PROCESS_TYPE",
            "FEOL_SHIPPING_FLAG",
            "FEOL_SHIPPING_FLAG_DTM",
            "CELL_SCRAP_FLAG",
            "CELL_SCRAP_FLAG_DTM",
            "FEOL_RECEIVE_DTM",
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
            "CELL_PRODUCTION_AREA",
        ],  # 移除 TFT_CHIP_ID_RRN, CURRENT_STAGE
        "BEOL": [
            "ARRAY_LOT_ID",
            "ARRAY_SITE_ID",
            "ARRAY_FAB_CODE",
            "ARRAY_CHIP_TYPE",
            "ARRAY_WO_ID",
            "ARRAY_PRODUCT_CODE",
            "ARRAY_ABBR_NO",
            "ARRAY_MODEL_NO",
            "ARRAY_PART_NO",
            "ARRAY_START_DATE",
            "ARRAY_FINAL_GRADE",
            "ARRAY_FINAL_TEST_TIME",
            "ARRAY_COMP_DTM",
            "ARRAY_SCRAP_FLAG",
            "ARRAY_SCRAP_FLAG_DTM",
            "ARRAY_SHIPPING_FLAG",
            "ARRAY_SHIPPING_FLAG_DTM",
            "ARRAY_TERMINATE_FLAG",
            "ARRAY_TERMINATE_FLAG_DTM",
            "ARRAY_RECYCLE_FLAG",
            "ARRAY_RECYCLE_FLAG_DTM",
            "ARRAY_INPUT_CANCEL_FLAG",
            "ARRAY_INPUT_CANCEL_FLAG_DTM",
            "ARRAY_WH_SHIPPING_DTM",
            "ARRAY_INPUT_PART_NO",
            "FEOL_SITE_ID",
            "FEOL_FAB_CODE",
            "FEOL_CHIP_TYPE",
            "FEOL_WO_ID",
            "FEOL_BATCH_ID",
            "FEOL_PRODUCT_CODE",
            "FEOL_ABBR_NO",
            "FEOL_MODEL_NO",
            "FEOL_PART_NO",
            "FEOL_START_DATE",
            "FEOL_SHIPPING_FLAG",
            "FEOL_SHIPPING_FLAG_DTM",
            "FEOL_INPUT_PART_NO",
            "FEOL_RECEIVE_DTM",
            "FEOL_WH_RECEIVE_DTM",
            "FEOL_WH_SHIPPING_DTM",
            "BEOL_SITE_ID",
            "BEOL_CHIP_TYPE",
            "BEOL_WO_ID",
            "BEOL_BATCH_ID",
            "TFT_GLASS_ID",
            "TFT_SUB_SHEET_ID",
            "TFT_CHIP_ID",
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
            "CELL_INPUT_PART_NO",
            "CELL_SOURCE_FAB_CODE",
            "CELL_PRODUCTION_AREA",
        ],  # 移除 TFT_CHIP_ID_RRN, CURRENT_STAGE
        "ALL": [
            # "CREATE_DTM",
            "ARRAY_SITE_ID",
            "CF_SITE_ID",
            "FEOL_SITE_ID",
            "BEOL_SITE_ID",
            "MODULE_SITE_ID",
            "ARRAY_WO_ID",
            "ARRAY_LOT_ID",
            "CF_WO_ID",
            "CF_LOT_ID",
            "CF_GLASS_ID",
            "CF_SUB_SHEET_ID",
            "CF_CHIP_ID",
            "FEOL_WO_ID",
            "FEOL_BATCH_ID",
            "BEOL_WO_ID",
            "BEOL_BATCH_ID",
            "TFT_GLASS_ID",
            "TFT_SUB_SHEET_ID",
            "TFT_CHIP_ID",
            "MODULE_WO_ID",
            "MODULE_LOT_ID",
            "ARRAY_PRODUCT_CODE",
            "ARRAY_ABBR_NO",
            "ARRAY_MODEL_NO",
            "ARRAY_PART_NO",
            "CF_PRODUCT_CODE",
            "CF_ABBR_NO",
            "CF_MODEL_NO",
            "CF_PART_NO",
            "FEOL_PRODUCT_CODE",
            "FEOL_ABBR_NO",
            "FEOL_MODEL_NO",
            "FEOL_PART_NO",
            "BEOL_PRODUCT_CODE",
            "BEOL_ABBR_NO",
            "BEOL_MODEL_NO",
            "BEOL_PART_NO",
            "MODULE_MODEL_NO",
            "MODULE_PART_NO",
            "ARRAY_1ST_GRADE",
            "ARRAY_1ST_TEST_TIME",
            "ARRAY_FINAL_GRADE",
            "ARRAY_FINAL_TEST_TIME",
            "ARRAY_SCRAP_FLAG",
            "ARRAY_SCRAP_FLAG_DTM",
            "ARRAY_SHIPPING_FLAG",
            "ARRAY_SHIPPING_FLAG_DTM",
            "CF_FINAL_GRADE",
            "CF_FINAL_TEST_TIME",
            "CF_SHIPPING_FLAG",
            "CF_SHIPPING_FLAG_DTM",
            "CELL_PROCESS_TYPE",
            "CELL_1ST_GRADE",
            "CELL_1ST_TEST_TIME",
            "CELL_1ST_TEST_MODE",
            "CELL_FINAL_GRADE",
            "CELL_FINAL_TEST_TIME",
            "CELL_FINAL_TEST_MODE",
            "CELL_FINAL_QA_JUDGE_TYPE",
            "CELL_FINAL_QA_JUDGE_CRIT",
            "CELL_FINAL_QA_GRADE",
            "CELL_FINAL_QA_TEST_TIME",
            "CELL_FMA_TEST_TIME",
            "CELL_SCRAP_FLAG",
            "CELL_SCRAP_FLAG_DTM",
            "FEOL_SHIPPING_FLAG",
            "FEOL_SHIPPING_FLAG_DTM",
            "BEOL_SHIPPING_FLAG",
            "BEOL_SHIPPING_FLAG_DTM",
            "MODULE_1ST_FD_GRADE",
            "MODULE_1ST_FD_TEST_TIME",
            "MODULE_1ST_FV_GRADE",
            "MODULE_1ST_FV_TEST_TIME",
            "MODULE_FINAL_FD_GRADE",
            "MODULE_FINAL_FD_TEST_TIME",
            "MODULE_FINAL_FV_GRADE",
            "MODULE_FINAL_FV_TEST_TIME",
            "MODULE_FINAL_OQC_GRADE",
            "MODULE_FINAL_OQC_TEST_TIME",
            "MODULE_FINAL_FMA_TEST_TIME",
            "MODULE_SCRAP_FLAG",
            "MODULE_SCRAP_FLAG_DTM",
            "GRADE_CHANGE_FLAG",
            "MODULE_SHIPPING_FLAG",
            "MODULE_SHIPPING_FLAG_DTM",
            "MODULE_SHIPPING_NO",
            # "UPDATE_DTM",
            "LOCAL_UPDATE_DTM",
            "ARRAY_START_DATE",
            "FEOL_START_DATE",
            "BEOL_START_DATE",
            "MODULE_START_DATE",
            "MODULE_PRODUCT_CODE",
            "ARRAY_CHIP_TYPE",
            "FEOL_CHIP_TYPE",
            "BEOL_CHIP_TYPE",
            "MODULE_CHIP_TYPE",
            "MODULE_PACKING_FLAG",
            "MODULE_PACKING_FLAG_DTM",
            "ARRAY_FAB_CODE",
            "CF_FAB_CODE",
            "FEOL_FAB_CODE",
            "BEOL_FAB_CODE",
            "MODULE_FAB_CODE",
            # "TFT_CHIP_ID_RRN",
            # "ARRAY_UPDATE_DTM",
            # "FEOL_UPDATE_DTM",
            # "BEOL_UPDATE_DTM",
            # "MODULE_UPDATE_DTM",
            "MODULE_FINAL_GRADE",
            "CURRENT_STAGE",
            # "EVENT_DTM",
            "OUTSOURCING_FLAG",
            "ARRAY_WH_SHIPPING_DTM",
            "FEOL_WH_RECEIVE_DTM",
            "FEOL_WH_SHIPPING_DTM",
            "BEOL_WH_RECEIVE_DTM",
            "BEOL_WH_SHIPPING_DTM",
            "MODULE_WH_RECEIVE_DTM",
            "MODULE_WH_SHIPPING_DTM",
            "HUB_RECEIVE_DTM",
            "HUB_SHIPPING_DTM",
            "AUO_FINAL_SHIPPING_DTM",
            "SHIPPING_UPDATE_DTM",
            "AUO_FINAL_SHIPPING_FLAG",
            "SHIPPING_BU",
            "DESIGN_BU",
            "OUT_FAB_CODE",
            "FEOL_RECEIVE_DTM",
            "BEOL_RECEIVE_DTM",
            "MODULE_RECEIVE_DTM",
            "ARRAY_COMP_DTM",
            "AUO_CHIP_ID",
            "CARTON_NO",
            "PALLET_NO",
            "DEST_SITE",
            "JI_SITE_ID",
            "JI_WO_ID",
            "JI_LOT_ID",
            "JI_MODEL_NO",
            "JI_PART_NO",
            "JI_SCRAP_FLAG",
            "JI_SCRAP_FLAG_DTM",
            "JI_SHIPPING_FLAG",
            "JI_SHIPPING_FLAG_DTM",
            "JI_SHIPPING_NO",
            "JI_START_DATE",
            "JI_PRODUCT_CODE",
            "JI_CHIP_TYPE",
            "JI_PACKING_FLAG",
            "JI_PACKING_FLAG_DTM",
            "JI_FAB_CODE",
            "JI_UPDATE_DTM",
            "JI_FINAL_GRADE",
            "JI_WH_RECEIVE_DTM",
            "JI_WH_SHIPPING_DTM",
            "JI_RECEIVE_DTM",
            "LCM_BEOL_SITE_ID",
            "LCM_BEOL_WO_ID",
            "LCM_BEOL_BATCH_ID",
            "LCM_BEOL_PRODUCT_CODE",
            "LCM_BEOL_ABBR_NO",
            "LCM_BEOL_MODEL_NO",
            "LCM_BEOL_PART_NO",
            "LCM_BEOL_SHIPPING_FLAG",
            "LCM_BEOL_SHIPPING_FLAG_DTM",
            "LCM_BEOL_START_DATE",
            "LCM_BEOL_CHIP_TYPE",
            "LCM_BEOL_FAB_CODE",
            "LCM_BEOL_UPDATE_DTM",
            "LCM_BEOL_WH_RECEIVE_DTM",
            "LCM_BEOL_WH_SHIPPING_DTM",
            "LCM_BEOL_RECEIVE_DTM",
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
            "CELL_PRODUCTION_AREA",
            "CELL_CASSETTE_BIN",
            "ARRAY_INPUT_PART_NO",
            "FEOL_INPUT_PART_NO",
            "BEOL_INPUT_PART_NO",
            "LCMBEOL_INPUT_PART_NO",
            "JI_INPUT_PART_NO",
            "MODULE_INPUT_PART_NO",
            "SET_FAB_CODE",
            "SET_SITE_ID",
            "SET_WO_ID",
            "SET_CHIP_TYPE",
            "SET_LOT_ID",
            "SET_MODEL_NO",
            "SET_PART_NO",
            "SET_INPUT_PART_NO",
            "SET_PRODUCT_CODE",
            "SET_RECEIVE_DTM",
            "SET_START_DATE",
            "SET_SCRAP_FLAG",
            "SET_SCRAP_FLAG_DTM",
            "SET_PACKING_FLAG",
            "SET_PACKING_FLAG_DTM",
            "SET_SHIPPING_FLAG",
            "SET_SHIPPING_FLAG_DTM",
            "SET_SHIPPING_NO",
            "SET_FINAL_GRADE",
            "SET_UPDATE_DTM",
            "SET_WH_SHIPPING_DTM",
            "CELL_FINAL_TEST_FAB_CODE",
            "CELL_FINAL_TEST_GRADE",
            "CELL_57_FINAL_TEST_FAB_CODE",
            "CELL_57_FINAL_TEST_PART_NO",
            "CELL_57_FINAL_TEST_TIME",
            "CELL_57_FINAL_TEST_GRADE",
            "MODULE_SFG_SITE_ID",
            "MODULE_SFG_WO_ID",
            "MODULE_SFG_LOT_ID",
            "MODULE_SFG_MODEL_NO",
            "MODULE_SFG_PART_NO",
            "MODULE_SFG_SCRAP_FLAG",
            "MODULE_SFG_SCRAP_FLAG_DTM",
            "MODULE_SFG_SHIPPING_FLAG",
            "MODULE_SFG_SHIPPING_FLAG_DTM",
            "MODULE_SFG_SHIPPING_NO",
            "MODULE_SFG_START_DATE",
            "MODULE_SFG_PRODUCT_CODE",
            "MODULE_SFG_CHIP_TYPE",
            "MODULE_SFG_PACKING_FLAG",
            "MODULE_SFG_PACKING_FLAG_DTM",
            "MODULE_SFG_FAB_CODE",
            "MODULE_SFG_UPDATE_DTM",
            "MODULE_SFG_FINAL_GRADE",
            "MODULE_SFG_WH_RECEIVE_DTM",
            "MODULE_SFG_WH_SHIPPING_DTM",
            "MODULE_SFG_RECEIVE_DTM",
            "MODULE_SFG_INPUT_PART_NO",
            "SUB_INVENTORY",
            "ARRAY_TERMINATE_FLAG",
            "ARRAY_TERMINATE_FLAG_DTM",
            "JUDGE_RESULT",
            "RESPONSIBILITY",
            "SHIPPING_PALLET_NO",
            "ARRAY_RECYCLE_FLAG",
            "ARRAY_INPUT_CANCEL_FLAG",
            "ARRAY_RECYCLE_FLAG_DTM",
            "ARRAY_INPUT_CANCEL_FLAG_DTM",
            "CELL_57_FINAL_TEST_MODEL_NO",
            "CELL_57_FINAL_TEST_ABBR_NO",
            "BEFORE_JI_TIME_INTERVAL",
            "CELL_SOURCE_FAB_CODE",
            "CELL_INPUT_PART_NO",
            "EPP_BOX_NO",
            # "AZ_DB_TIME",
        ],
    }
    return _dict

# COMMAND ----------

def check_cols(df, stage):
  stage_cols_lists = get_check_cols().get(stage)

  selected_columns = []
  for col in stage_cols_lists:
    if df.schema[col].dataType == StringType():  
        col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    elif df.schema[col].dataType == TimestampType():  
        col_expr =  F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.date_format(F.col(f"S.{col}"), "yyyy-MM-dd HH:mm:ss") != F.date_format(F.col(f"T.{col}"), "yyyy-MM-dd HH:mm:ss")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    elif df.schema[col].dataType == DecimalType():  
        col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    else:  
        col_expr = F.when((F.col(f"T.{col}") == 0) & (F.col(f"S.{col}").isNull()), "NULL").when(((F.col(f"T.{col}") != 0) & (F.col(f"S.{col}").isNull())) | (F.round(F.col(f"S.{col}")) != F.round(F.col(f"T.{col}"))) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")
    selected_columns.extend([F.col(f"T.{col}"), F.col(f"S.{col}"), col_expr]) 

  filter_condition = ' OR '.join([f"{col}_Flag = 'NG'" for col in stage_cols_lists])
  return df.select(*PK_COLS_LIST + selected_columns).where(filter_condition) 

def select_stage_cols(df, stage):
  stage_cols_lists = get_check_cols().get(stage)
  return df.select([F.col(c).cast(StringType()) for c in PK_COLS_LIST] + stage_cols_lists)

def filter_update_dtm(df, stage):
  return df.filter(f"UPDATE_DTM >= '{START_TIME}' AND UPDATE_DTM < '{END_TIME}'") if stage == 'ALL' else df.filter(f"{stage}_UPDATE_DTM >= '{START_TIME}' AND {stage}_UPDATE_DTM < '{END_TIME}'") 

# COMMAND ----------

def get_sorce_dataframe(stage):
  return spark.table("auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods").transform(select_stage_cols, stage)
  
def get_target_dataframe(stage):
  return spark.table("auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_master_ods").transform(filter_update_dtm, stage).transform(select_stage_cols, stage)
  # .transform(filter_update_dtm, stage).transform(select_stage_cols, stage)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Main

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def __init__(self, *args, **kwargs):  
        super(TestHelpers, self).__init__(*args, **kwargs)  
        self.stage = os.getenv('STAGE') 

    def setUp(self):  
      self.sorce_df = get_sorce_dataframe(self.stage)  
      self.target_df = get_target_dataframe(self.stage)
      # self.sorce_df.cache()
      # self.target_df.cache()

    def test_count(self):
      self.assertEqual(self.sorce_df.count(), self.target_df.count()) 

    def join_stage_col(self):
      check_df = (
        self.target_df.alias('T')
        .join(self.sorce_df.alias('S'), on=PK_COLS_LIST, how="left")
        .transform(check_cols, self.stage)
      )
      check_df.cache()
      check_df.display()
      return check_df

    def test_stage_data_by_fab(self):
      check_df = self.join_stage_col()

      self.stage = self.stage if self.stage != 'ALL' else 'ARRAY'
      fab_codes = self.target_df.select(f"{self.stage}_FAB_CODE").distinct().collect() 
      fab_codes = [int(row[f"{self.stage}_FAB_CODE"].to_integral_value()) for row in fab_codes]

      for fab_code in fab_codes:
        print(f"FAB_CODE: {fab_code}")
        fab_df = check_df.filter(f"S.{self.stage}_FAB_CODE == {fab_code}")
        fab_df.display()

      # if fab_df.count() > 0:
      #   self.fail(f'Dataframes are not equal')  

# COMMAND ----------

if PROCESS_STAGE == 'ARRAY':
  os.environ['STAGE'] = PROCESS_STAGE
  r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

if PROCESS_STAGE == 'FEOL':
  os.environ['STAGE'] = PROCESS_STAGE
  r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

if PROCESS_STAGE == 'BEOL':
  os.environ['STAGE'] = PROCESS_STAGE
  r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

if PROCESS_STAGE == 'ALL':
  os.environ['STAGE'] = PROCESS_STAGE
  r = unittest.main(argv=[''], verbosity=2, exit=False)
