# Databricks notebook source
import os
import pytz
import unittest
from datetime import datetime, time, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType, DecimalType, DateType

from util.email import send_mail

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
  # CHECK_TYPE = dbutils.widgets.get("CHECK_TYPE")
except:
  if IS_TEST:
    START_TIME = '2025-01-20 00:00:00'
    END_TIME = '2025-01-20 23:59:59'
    # CHECK_TYPE = 'scrap'
    
if START_TIME == '' and END_TIME == '':
  now = datetime.now(TIME_ZONE) - timedelta(days=1) 
  new_time = now - timedelta(minutes=60)

  rounded_now = round_time(now, 60)  
  rounded_new_time = round_time(new_time, 60)  
    
  START_TIME = rounded_new_time.strftime('%Y-%m-%d 00:00:00')  
  END_TIME = rounded_now.strftime('%Y-%m-%d 23:59:59')  

print(f"START_TIME = {START_TIME}")
print(f"END_TIME = {END_TIME}")

# COMMAND ----------

PK_COLS_LIST = ['TFT_CHIP_ID_RRN', 'PROCESS_STAGE', 'FAB_CODE']

JOIN_STR = """
  T.PROCESS_STAGE = S.PROCESS_STAGE
  and CAST(T.TFT_CHIP_ID_RRN AS STRING) = CAST(S.TFT_CHIP_ID_RRN AS STRING)
  and T.FAB_CODE = S.FAB_CODE
"""

# COMMAND ----------

def get_check_cols(CHECK_TYPE):
  if CHECK_TYPE == 'scrap_all_col':
    _dict = [
          # "CREATE_DTM",
          # "FAB_CODE",
          # "PROCESS_STAGE",
          "ARRAY_LOT_ID",
          "WO_ID",
          "WO_TYPE",
          "PART_NO",
          "MODEL_NO",
          "SCRAP_MFG_DAY",
          "TFT_CHIP_ID",
          "MODULE_FINAL_GRADE",
          "CELL_GRADE",
          "SCRAP_CODE",
          "SCRAP_DESC",
          # "UPDATE_DTM",
          # "TFT_CHIP_ID_RRN",
          "LOT_TYPE",
          "EVENT_DTM",
          "SITE_TYPE",
          "SITE_ID",
          "UNSCRAP_FLAG",
          "UNSCRAP_FLAG_DTM",
          # "AZ_DB_TIME"
      ]
  else:
    _dict = [
          # "CREATE_DTM",
          # "FAB_CODE",
          # "PROCESS_STAGE",
          "ARRAY_LOT_ID",
          "WO_ID",
          "WO_TYPE",
          "PART_NO",
          "MODEL_NO",
          "SCRAP_MFG_DAY",
          "TFT_CHIP_ID",
          "MODULE_FINAL_GRADE",
          # "CELL_GRADE",
          "SCRAP_CODE",
          # "SCRAP_DESC",
          # "UPDATE_DTM",
          # "TFT_CHIP_ID_RRN",
          "LOT_TYPE",
          "EVENT_DTM",
          "SITE_TYPE",
          "SITE_ID",
          "UNSCRAP_FLAG",
          "UNSCRAP_FLAG_DTM",
          # "AZ_DB_TIME"
      ]
  return _dict

# COMMAND ----------

def check_cols(df):
  stage_cols_lists = get_check_cols(CHECK_TYPE)

  selected_columns = []
  for col in stage_cols_lists:
    if df.schema[col].dataType == StringType():  
        col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    elif df.schema[col].dataType == TimestampType():  
        col_expr =  F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.date_format(F.col(f"S.{col}"), "yyyy-MM-dd HH:mm:ss") != F.date_format(F.col(f"T.{col}"), "yyyy-MM-dd HH:mm:ss")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    elif df.schema[col].dataType == DateType():  
        col_expr =  F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.date_format(F.col(f"S.{col}"), "yyyy-MM-dd") != F.date_format(F.col(f"T.{col}"), "yyyy-MM-dd")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    elif df.schema[col].dataType == DecimalType():  
        col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
    else:  
        col_expr = F.when((F.col(f"T.{col}") == 0) & (F.col(f"S.{col}").isNull()), "NULL").when(((F.col(f"T.{col}") != 0) & (F.col(f"S.{col}").isNull())) | (F.round(F.col(f"S.{col}")) != F.round(F.col(f"T.{col}"))) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")
    selected_columns.extend([F.col(f"T.{col}"), F.col(f"S.{col}"), col_expr]) 

  filter_condition = ' OR '.join([f"{col}_Flag = 'NG'" for col in stage_cols_lists])
  return df.select([F.col(f"T.{col}") for col in PK_COLS_LIST] + selected_columns).where(filter_condition) 

def select_stage_cols(df, is_source=False):
  stage_cols_lists = get_check_cols(CHECK_TYPE)
  if is_source:
    return df.select([F.col(c).cast(StringType()) for c in PK_COLS_LIST] + stage_cols_lists)
  else:
    return df.select([F.col(c).cast(StringType()) for c in PK_COLS_LIST] + stage_cols_lists)

def filter_time(df):
  return df.filter(f"SCRAP_MFG_DAY >= '{START_TIME}' AND SCRAP_MFG_DAY <= '{END_TIME}'")


# COMMAND ----------

def get_sorce_dataframe():
  return spark.table("auofab.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_scrap_ods").transform(filter_time).transform(select_stage_cols, is_source=True)
  
def get_target_dataframe():
  return spark.table("auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_scrap_ods").transform(filter_time).transform(select_stage_cols, is_source=False)

# COMMAND ----------

# class TestHelpers(unittest.TestCase):
#     def __init__(self, *args, **kwargs):  
#         super(TestHelpers, self).__init__(*args, **kwargs)  

#     def setUp(self):  
#       self.sorce_df = get_sorce_dataframe()  
#       self.target_df = get_target_dataframe()
#       # self.sorce_df.cache()
#       # self.target_df.cache()

#     def test_count(self):
#       self.assertEqual(self.sorce_df.count(), self.target_df.count()) 

#     def join_stage_col(self):
#       check_df = (
#         self.target_df.alias('T')
#         .join(self.sorce_df.alias('S'), on=F.expr(JOIN_STR), how="left")
#         .transform(check_cols)
#       )
#       # check_df.cache()
#       check_df.display()
#       return check_df

#     def test_stage_data_by_fab(self):
#       check_df = self.join_stage_col()
#       fab_codes = self.target_df.select("FAB_CODE", "SITE_ID").distinct().collect() 
#       fab_codes = [(int(float(row["FAB_CODE"])), row["SITE_ID"]) for row in fab_codes]

#       for fab_code, site_id in fab_codes:
#         print(f"FAB_CODE: {fab_code}, SITE_ID: {site_id}")
#         fab_df = check_df.filter((F.col("T.FAB_CODE") == fab_code))
#         fab_df.display()

#       # fab_df = check_df.filter(f"T.MFG_FAB_CODE == '111690'")
#       # fab_df.display()

# COMMAND ----------

# if CHECK_TYPE == 'scrap':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# DBTITLE 1,Check All Col
CHECK_TYPE = 'scrap_all_col'
sorce_df = get_sorce_dataframe()  
target_df = get_target_dataframe()

check_df = (
  target_df.alias('T')
  .join(sorce_df.alias('S'), on=F.expr(JOIN_STR), how="left")
  .transform(check_cols)
)
check_df.cache()
check_df.display()

fab_codes = target_df.select("FAB_CODE", "SITE_ID").distinct().collect() 
fab_codes = [(int(float(row["FAB_CODE"])), row["SITE_ID"]) for row in fab_codes]

for fab_code, site_id in fab_codes:
  print(f"FAB_CODE: {fab_code}, SITE_ID: {site_id}")
  fab_df = check_df.filter((F.col("T.FAB_CODE") == fab_code))
  fab_df.display()

# sand_mail
check_all_col_df = check_df.groupby("T.FAB_CODE", "T.SITE_ID", "T.PROCESS_STAGE").count()
check_all_col_html = check_all_col_df.toPandas().to_html()

# COMMAND ----------

# DBTITLE 1,Check Filter Col
CHECK_TYPE = 'scrap_filter_col'
sorce_df = get_sorce_dataframe()  
target_df = get_target_dataframe()

check_df = (
  target_df.alias('T')
  .join(sorce_df.alias('S'), on=F.expr(JOIN_STR), how="left")
  .transform(check_cols)
)

check_df.cache()
check_df.display()

fab_codes = target_df.select("FAB_CODE", "SITE_ID").distinct().collect() 
fab_codes = [(int(float(row["FAB_CODE"])), row["SITE_ID"]) for row in fab_codes]

for fab_code, site_id in fab_codes:
  print(f"FAB_CODE: {fab_code}, SITE_ID: {site_id}")
  fab_df = check_df.filter((F.col("T.FAB_CODE") == fab_code))
  fab_df.display()

# sand_mail
check_filter_col_df = check_df.groupby("T.FAB_CODE", "T.SITE_ID", "T.PROCESS_STAGE").count()
check_filter_col_html = check_filter_col_df.toPandas().to_html()

# COMMAND ----------

if check_filter_col_df.count() > 0 or check_all_col_df.count():
  send_mail(
    data_mail = f"""<B><font color='blue'>比對所有欄位</B><br/>{check_all_col_html}<br/><B><font color='blue'>比對過濾欄位</B><br/>{check_filter_col_html}<br/>
    """,
    send_from = f"FBK Scrap 資料比對結果",
    send_to = ['Cory.CC.Cheng@auo.com'],
    send_subject = f"FBK Scrap 驗證不一致: START_TIME = {START_TIME}, END_TIME = {END_TIME}"
  )
else:
  print(f"資料比對一致")
