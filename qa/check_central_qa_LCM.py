# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 檢查目的端table資料是否都有在來源資料(未驗證資料欄位)

# COMMAND ----------

# MAGIC %md
# MAGIC #比對地上雲資料表與fusion資料表
# MAGIC auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_test_ods、auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods

# COMMAND ----------

# MAGIC %md
# MAGIC #以下為Temp測試程式

# COMMAND ----------

# MAGIC %md
# MAGIC # 驗證

# COMMAND ----------

import os
import pytz
import unittest
from datetime import datetime, time, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType, DecimalType, DateType

# COMMAND ----------

def round_time(dt, round_to=15):  
  dt = dt.replace(tzinfo=None)  
  seconds_past_hour = (dt - dt.replace(minute=0, second=0, microsecond=0)).total_seconds()  
  rounding = (seconds_past_hour + round_to*30) // (round_to*60) * (round_to*60)  
  return dt + timedelta(seconds=rounding-seconds_past_hour)  

# COMMAND ----------

IS_TEST = True
TIME_ZONE = pytz.timezone('Asia/Taipei')

try:
  START_TIME = dbutils.widgets.get("CHECK_START_TIME")
  END_TIME = dbutils.widgets.get("CHECK_END_TIME")
  CHECK_TYPE = dbutils.widgets.get("CHECK_TYPE")
except:
  if IS_TEST:
    START_TIME = '2025-02-01 00:00:00'
    END_TIME = '2025-02-15 00:00:00'
    CHECK_TYPE = 'BRISA'
    
if START_TIME == '' and END_TIME == '':
  now = datetime.now(TIME_ZONE) - timedelta(days=1) 
  new_time = now - timedelta(minutes=60)

  rounded_now = round_time(now, 60)  
  rounded_new_time = round_time(new_time, 60)  
    
  START_TIME = rounded_new_time.strftime('%Y-%m-%d %H:%M:00')  
  END_TIME = rounded_now.strftime('%Y-%m-%d %H:%M:00')  

print(f"CHECK_TYPE={CHECK_TYPE}")
print(f"START_TIME = {START_TIME}")
print(f"END_TIME = {END_TIME}")

# COMMAND ----------

# PK_COLS_LIST = ['FAB_CODE', 'PROCESS_STAGE', 'TFT_CHIP_ID_RRN', 'TEST_TYPE', 'TEST_TIME']

PK_COLS_LIST = ['FAB_CODE', 'PROCESS_STAGE', 'TFT_CHIP_ID_RRN',  'TEST_TIME']


JOIN_STR = """
  T.FAB_CODE = S.FAB_CODE
  and T.PROCESS_STAGE = S.PROCESS_STAGE 
  and T.TEST_TIME = S.TEST_TIME
  and CAST(T.TFT_CHIP_ID_RRN AS STRING) = CAST(S.TFT_CHIP_ID_RRN AS STRING)
"""

# COMMAND ----------

def get_check_cols():
  _dict = [
        # "PROCESS_STAGE",
        # "FAB_CODE",
        # "CREATE_DTM",
        # "SITE_TYPE",
        "SITE_ID",
        # "TOOL_ID",
        # "OP_ID",
        # "SHIFT",
        "ARRAY_LOT_ID",
        # "TFT_GLASS_ID",
        # "TFT_SUB_SHEET_ID",
        "TFT_CHIP_ID",
        # "PRODUCT_CODE",
        # "ABBR_NO",
        # "MODEL_NO",
        # "PART_NO",
        # "TEST_TIME",
        # "TEST_USER",
        "TEST_TYPE",
        # "TEST_MODE",
        # "MFG_DAY",
        # "JUDGE_FLAG",
        # "PRE_GRADE",
        "GRADE",
        # "GRADE_CHANGE_FLAG",
        # "JUDGE_CNT",
        # "DEFECT_TYPE",
        # "DEFECT_CODE",
        # "DEFECT_CODE_DESC",
        # "DEFECT_VALUE",
        # "NOTE_FLAG",
        # "NOTE",
        # "UPDATE_DTM",
        # "FIRST_YIELD_FLAG",
        # "FINAL_YIELD_FLAG",
        # "CHIP_TYPE",
        # "PRE_DEFECT_CODE",
        # "PRE_DEFECT_CODE_DESC",
        # "TFT_CHIP_ID_RRN",
        # "CUR_LOT_ID",
        # "AUO_CHIP_ID",
        # "OP_CAT",
        # "NEXT_OP_ID",
        # "IS_OUTPUT_NG",
        # "PNL_REPR_CNT",
        # "INPUT_PART_NO",
        # "DISASSEMBLY_TIME",
        # "LVL",
        # "AZ_LM_TIME"
    ]
  return _dict

# COMMAND ----------

def check_cols(df):
  stage_cols_lists = get_check_cols()

  selected_columns = []
  for col in stage_cols_lists:
    if df.schema[col].dataType == StringType():  
        col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag_C")  
    elif df.schema[col].dataType == TimestampType():  
        col_expr =  F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.date_format(F.col(f"S.{col}"), "yyyy-MM-dd HH:mm:ss") != F.date_format(F.col(f"T.{col}"), "yyyy-MM-dd HH:mm:ss")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag_C")  
    elif df.schema[col].dataType == DateType():  
        col_expr =  F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.date_format(F.col(f"S.{col}"), "yyyy-MM-dd HH:mm:ss") != F.date_format(F.col(f"T.{col}"), "yyyy-MM-dd HH:mm:ss")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag_C")  
    elif df.schema[col].dataType == DecimalType():  
        col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag_C")  
    else:  
        col_expr = F.when((F.col(f"T.{col}") == 0) & (F.col(f"S.{col}").isNull()), "NULL").when(((F.col(f"T.{col}") != 0) & (F.col(f"S.{col}").isNull())) | (F.round(F.col(f"S.{col}")) != F.round(F.col(f"T.{col}"))) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag_C")
    selected_columns.extend([F.col(f"T.{col}"), F.col(f"S.{col}"), col_expr]) 

  filter_condition = ' OR '.join([f"{col}_Flag_C = 'NG'" for col in stage_cols_lists])

  return df.select([F.col(f"T.{col}") for col in PK_COLS_LIST] + selected_columns).where(filter_condition) 

def select_stage_cols(df, is_source=False):
  stage_cols_lists = get_check_cols()
  if is_source:
    return df.select([F.col(c).cast(StringType()) for c in PK_COLS_LIST] + stage_cols_lists)
  else:
    return df.select([F.col(c).cast(StringType()) for c in PK_COLS_LIST] + stage_cols_lists)

def filter_time(df):
  return df.filter(f"TEST_TIME >= '{START_TIME}' AND TEST_TIME < '{END_TIME}'")

def filter_process_stage_and_fab(df, check_type):
  if check_type == 'ARRAY':
    return df.filter(f"PROCESS_STAGE in ('ARRAY')")
  elif check_type == 'FEOL':
    return df.filter(f"PROCESS_STAGE in ('FEOL')")
  elif check_type == 'BEOL':
    return df.filter(f"PROCESS_STAGE in ('BEOL') AND FAB_CODE NOT IN (117114, 117118)")
  elif check_type == 'MODULE':
    return df.filter(f"PROCESS_STAGE in ('MODULE') AND FAB_CODE IN (105000, 105060, 105160, 115000, 117011, 115060, 115100, 115200, 115160, 117006, 111690, 101700)")
  elif check_type == 'LCM_MODULE':
    return df.filter(f"PROCESS_STAGE in ('MODULE') AND FAB_CODE IN (117011,117006,105060,105160,115000,115060,115160,115200,115270,111690)")  
  elif check_type == 'LCM_BEOL':
    return df.filter(f"PROCESS_STAGE in ('BEOL') AND FAB_CODE IN (115000, 115060)")
  elif check_type == 'EDI':    
    return df.filter(f"FAB_CODE IN (117540,117475,117118, 117027, 117511, 117830, 116500, 117114, 117385, 117133, 117021, 189001, 189002, 117540, 117475, 127100)")
  elif check_type == 'EDI_BEOL':
    return df.filter(f"PROCESS_STAGE in ('BEOL') AND FAB_CODE IN (117114, 117118)")
  elif check_type == 'BRISA':
    return df.filter(f"FAB_CODE IN (115400, 117013, 117381, 117130, 117912, 117926, 102455, 117133, 117021, 117330, 117410, 117480, 117520, 117940, 117918, 117920, 117919)")
  else:
    return df.filter(f"""PROCESS_STAGE NOT IN ('ARRAY', 'FEOL', 'BEOL') AND FAB_CODE NOT IN (115400, 117013, 117381, 117130, 117912, 117926, 102455, 117133, 117021, 117330, 117410, 117480, 117520, 117940, 117918, 117920, 117919, 117118, 117027, 117511, 117830, 116500, 117114, 117385, 117133, 117021, 189001, 189002, 117540, 117475, 127100) AND (PROCESS_STAGE in ('MODULE') AND FAB_CODE IN (105000, 105060, 105160, 115000, 117011, 115060, 115100, 115200, 115160, 117006, 111690, 101700))""")



# COMMAND ----------

def get_sorce_dataframe():
  return spark.table("auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods").transform(filter_time).transform(filter_process_stage_and_fab, CHECK_TYPE).transform(select_stage_cols, is_source=True)
  
def get_target_dataframe():
  return spark.table("auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods").transform(filter_time).transform(filter_process_stage_and_fab, CHECK_TYPE).transform(select_stage_cols, is_source=False)

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def __init__(self, *args, **kwargs):  
        super(TestHelpers, self).__init__(*args, **kwargs)       
    def setUp(self):     
      self.sorce_df = get_sorce_dataframe()  
      self.target_df = get_target_dataframe()
      # self.sorce_df.cache()
      # self.target_df.cache()

    def test_count(self):
      self.assertEqual(self.sorce_df.count(), self.target_df.count())     

    def join_stage_col(self):
      check_df = (
        self.target_df.alias('T')
        .join(self.sorce_df.alias('S'), on=F.expr(JOIN_STR), how="left")
        .transform(check_cols)
      )
      # check_df.cache()      
      check_df.display()
      return check_df

    def test_stage_data_by_fab(self):
      check_df = self.join_stage_col()      
      fab_codes = self.target_df.select("FAB_CODE", "SITE_ID").distinct().collect() 
      fab_codes = [(int(float(row["FAB_CODE"])), row["SITE_ID"]) for row in fab_codes]

      for fab_code, site_id in fab_codes[:5]:
        print(f"FAB_CODE: {fab_code}, SITE_ID: {site_id}")
        fab_df = check_df.filter((F.col("T.FAB_CODE") == fab_code) & (F.col("T.SITE_ID") == site_id))
        fab_df.display()
      # fab_df = check_df.filter(f"T.MFG_FAB_CODE == '111690'")
      # fab_df.display()

# COMMAND ----------

if CHECK_TYPE == 'BRISA':
    r = unittest.main(argv=[''], verbosity=2, exit=False)
