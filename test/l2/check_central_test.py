# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 檢查目的端table資料是否都有在來源資料(未驗證資料欄未)

# COMMAND ----------

# DBTITLE 1,config
# process_stage = "ARRAY" # ARRAY CELL MODULE CF

# if process_stage == "ARRAY":
#   sqlcond_ps = ["ARRAY"]
#   fab_code_dict = {
#   "L3C": 101100,
#   "L3D": 101160,
#   "L4A": 101200,
#   # "L4B": 161210,
#   # "L5A": 101300,
#   "L5B": 101360,
#   "L5C": 101500,
#   "L5D": 101560,
#   "L6A": 101600,
#   "L6B": 101660,
#   "L6K": 111690,
#   "L7A": 101700,
#   "L7B": 101760,
#   "L8A": 101800,
#   "L8B": 101860
#   }
# elif process_stage == "CELL":
#   sqlcond_ps = ["BEOL", "FEOL", "CELL"]
#   fab_code_dict = {
#   "L3C": 101100,
#   # "L3D": 101160,
#   # "L4A": 101200,
#   # "L4B": 161210,
#   # "L5A": 101300,
#   "L5B": 101360,
#   "L5C": 101500,
#   "L5D": 101560,
#   "L6A": 101600,
#   "L6B": 101660,
#   "L6K": 111690,
#   "L7A": 101700,
#   # "L7B": 101760,
#   "L8A": 101800,
#   "L8B": 101860
#   }  
# # 串成in sql 條件
# formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)

# START_UPDATE_DTM = '2024-12-11 00:00:00'  
# END_UPDATE_DTM = '2024-12-12 06:00:00'

# COMMAND ----------

# DBTITLE 1,比對 sql
# table_tag = f"h_dax_fbk_test_ods"

# #來源表sql 
# sql_template = """

# SELECT CREATE_DTM, UPDATE_DTM, TFT_CHIP_ID_RRN, SITE_TYPE, SITE_ID, TOOL_ID, OP_ID, SHIFT, ARRAY_LOT_ID, TFT_GLASS_ID, TFT_SUB_SHEET_ID, TFT_CHIP_ID, PRODUCT_CODE, ABBR_NO, MODEL_NO, PART_NO, TEST_TIME, TEST_USER, TEST_TYPE, TEST_MODE, MFG_DAY, JUDGE_FLAG, PRE_GRADE, GRADE, GRADE_CHANGE_FLAG, JUDGE_CNT, DEFECT_TYPE, DEFECT_CODE, DEFECT_CODE_DESC, DEFECT_VALUE, NOTE_FLAG, NOTE, FIRST_YIELD_FLAG, FINAL_YIELD_FLAG, CHIP_TYPE, PRE_DEFECT_CODE, PRE_DEFECT_CODE_DESC, CUR_LOT_ID, PROCESS_STAGE, FAB_CODE
# FROM
# ( 
# SELECT ROW_NUMBER() OVER(PARTITION BY FAB_CODE, PROCESS_STAGE, TFT_CHIP_ID_RRN, TEST_TYPE, TEST_TIME ORDER BY UPDATE_DTM DESC) AS INDEX,*    
# FROM auofab.itgp_eng.{key}_{process_stage}_{table_tag}
# WHERE FAB_CODE = '{value}'  
#       AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values})
#       AND UPDATE_DTM >= '{START_UPDATE_DTM}'  
#       AND UPDATE_DTM < '{END_UPDATE_DTM}'
# )
# WHERE INDEX=1
# """
# # 生成 SQL 查询字符串
# query_list = []
# for key, value in fab_code_dict.items():
#     query = sql_template.format(key=key, value=value,process_stage=process_stage.lower(), START_UPDATE_DTM=START_UPDATE_DTM, END_UPDATE_DTM=END_UPDATE_DTM, formatted_filter_ps_values=formatted_filter_ps_values,table_tag=table_tag)
#     query_list.append(query)

# sql_source_str = ' UNION '.join(f"{value}" for value in query_list) 

# sql_fusion_str = f"""
# SELECT *     
# FROM auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_{table_tag}
# WHERE TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values})
# """

# print(f"Source SQL:{sql_source_str}")
# print(f"Fusion SQL:{sql_fusion_str}")


# COMMAND ----------

# DBTITLE 1,驗證資料
# PK_COLS_LIST = ['PROCESS_STAGE','FAB_CODE','TFT_CHIP_ID','ARRAY_LOT_ID','TEST_TIME']
# df_source = spark.sql(sql_source_str)
# df_fusion = spark.sql(sql_fusion_str)

# print("Source df")
# df_source.display()
# print("Fusion df")
# df_fusion.display()

# # 已來源資料為主
# df_merged = df_source.join(df_fusion, on=PK_COLS_LIST, how='left')

# # 列出 df_source 中存在而不在 df_fusion 中的資料
# df_result = df_merged.filter(df_fusion[PK_COLS_LIST[0]].isNull())  # 检查 df_fusion 的某列是否为空

# count = df_result.count()
# if count > 0:
#   df_result.display()
#   print(f"process_stage={process_stage},time:{START_UPDATE_DTM} - {END_UPDATE_DTM}, 不一致筆數, count={df_result.count()}")
# else:
#   print(f"process_stage={process_stage},time:{START_UPDATE_DTM} - {END_UPDATE_DTM}, 沒有不一致")

# COMMAND ----------

# MAGIC %md
# MAGIC #比對地上雲資料表與fusion資料表
# MAGIC auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_test_ods、auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods

# COMMAND ----------

# DBTITLE 1,check function
# from pyspark.sql import functions as F
# from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType, DecimalType, ShortType

# def check_cols(df):
#   #比對欄位
#   stage_cols_lists = ['FIRST_YIELD_FLAG', 'FINAL_YIELD_FLAG','DEFECT_CODE', 'GRADE', 'GRADE_CHANGE_FLAG']

#   selected_columns = []
#   # col_exprs = []  
#   # for col in stage_cols_lists:
#   #   col_expr = F.when((F.col(f"S.{col}") == 0) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"S.{col}") != 0) & (F.col(f"T.{col}").isNull())) | (F.round(F.col(f"S.{col}")) != F.round(F.col(f"T.{col}"))), "NG").otherwise(f"OK").alias(f"{col}_Flag")
#   #   # col_exprs.append(col_expr) 
#   #   selected_columns.extend([F.col(f"S.{col}"), F.col(f"T.{col}"), col_expr])

#   for col in stage_cols_lists:
#     if df.schema[col].dataType == StringType():  
#         col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
#     elif df.schema[col].dataType == TimestampType():  
#         col_expr =  F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.date_format(F.col(f"S.{col}"), "yyyy-MM-dd HH:mm:ss") != F.date_format(F.col(f"T.{col}"), "yyyy-MM-dd HH:mm:ss")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
#     elif df.schema[col].dataType == DecimalType():  
#         col_expr = F.when((F.col(f"S.{col}").isNull()) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"T.{col}").isNotNull()) & (F.col(f"S.{col}").isNull())) | (F.col(f"S.{col}") != F.col(f"T.{col}")) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")  
#     else:  
#         col_expr = F.when((F.col(f"T.{col}") == 0) & (F.col(f"S.{col}").isNull()), "NULL").when(((F.col(f"T.{col}") != 0) & (F.col(f"S.{col}").isNull())) | (F.round(F.col(f"S.{col}")) != F.round(F.col(f"T.{col}"))) | ((F.col(f"T.{col}").isNull()) & (F.col(f"S.{col}").isNotNull())), "NG").otherwise("OK").alias(f"{col}_Flag")
#     selected_columns.extend([F.col(f"T.{col}"), F.col(f"S.{col}"), col_expr])   
  
#   # print(selected_columns)

#   filter_condition = ' OR '.join([f"{col}_Flag like 'NG' " for col in stage_cols_lists])
#   # if stage != 'MODULE':
#   #   return df.select(*PK_COLS_LIST + [f'S.{get_stage_part_cols_str(stage)}', f'T.{get_stage_part_cols_str(stage)}'] + selected_columns).where(filter_condition) 
#   # else:  
#   return df.select(*PK_COLS_LIST + selected_columns).where(filter_condition) 


# COMMAND ----------

# DBTITLE 1,比對資料
# table_tag = f"h_dax_fbk_test_ods"
# process_stage = "CELL" # ARRAY CELL
# START_UPDATE_DTM = '2024-12-14 08:00:00'  
# END_UPDATE_DTM = '2024-12-14 10:00:00'
# PK_COLS_LIST = ['PROCESS_STAGE','FAB_CODE','TFT_CHIP_ID_RRN','TEST_TYPE','TEST_TIME']

# if process_stage == "ARRAY":
#   sqlcond_ps = ["ARRAY"]
#   master_fabcode_col = "FEOL_FAB_CODE"
#   fab_code_dict = {
#   "L3C": 101100,
#   "L3D": 101160,
#   "L4A": 101200,
#   # "L4B": 161210,
#   # "L5A": 101300,
#   "L5B": 101360,
#   "L5C": 101500,
#   "L5D": 101560,
#   "L6A": 101600,
#   "L6B": 101660,
#   "L6K": 111690,
#   "L7A": 101700,
#   "L7B": 101760,
#   "L8A": 101800,
#   "L8B": 101860
#   }
# elif process_stage == "CELL":
#   sqlcond_ps = ["BEOL", "FEOL", "CELL"]
#   master_fabcode_col = "MODULE_FAB_CODE"
#   fab_code_dict = {
#   "L3C": 101100,
#   # "L3D": 101160,
#   # "L4A": 101200,
#   # "L4B": 161210,
#   # "L5A": 101300,
#   "L5B": 101360,
#   "L5C": 101500,
#   "L5D": 101560,
#   "L6A": 101600,
#   "L6B": 101660,
#   "L6K": 111690,
#   "L7A": 101700,
#   # "L7B": 101760,
#   "L8A": 101800,
#   "L8B": 101860
#   } 

# formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)
# formatted_filter_fabcode_values = ', '.join(str(value) for value in fab_code_dict.values())

# sql_source_str = f"""
# SELECT T.*
# FROM (SELECT * 
#       FROM auofab.prod_dw.dt_all_s_all_dw_at_feedback_{table_tag}
#       WHERE 1=1 
#             AND FAB_CODE in ({formatted_filter_fabcode_values})
#             AND TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' 
#             AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values})  ) T 
# JOIN  auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_master_ods M on T.TFT_CHIP_ID_RRN = M.TFT_CHIP_ID_RRN       
# WHERE M.{master_fabcode_col} IS NOT NULL
# """

# sql_fusion_str = f"""
# SELECT * 
# FROM auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_{table_tag}
# WHERE 1=1 
#       --AND FAB_CODE = '101860' 
#       AND FAB_CODE in ({formatted_filter_fabcode_values})
#       AND TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' 
#       AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values}) 
# """
# print(f"Source SQL:{sql_source_str}")
# print(f"Fusion SQL:{sql_fusion_str}")

# df_source = spark.sql(sql_source_str).withColumn("TEST_TYPE", F.when(
#                 F.col("TEST_TYPE").isNull(),
#                 F.lit('-')).otherwise(F.col("TEST_TYPE")))
# df_fusion = spark.sql(sql_fusion_str)
# # df_source.cache()
# # df_fusion.cache()

# print(f"Source df count={df_source.count()}")
# df_source.display()
# print(f"Fusion df count={df_fusion.count()}")
# df_fusion.display()


# df = df_source.alias('S').join(df_fusion.alias('T'), on=PK_COLS_LIST , how='left')
# check_df = df.transform(check_cols)
# # check_df.cache()
# print(f"{process_stage} 資料比對，不一致count={check_df.count()}")
# check_df.display()

# filter_fab_code_str = "fab_code not in (111690) "
# check_df = check_df.filter(f"{filter_fab_code_str}")
# print(f"{process_stage} 資料比對(過濾L6K)，不一致 count={check_df.count()}")
# check_df.display()
 

# COMMAND ----------

# MAGIC %md
# MAGIC #以下為Temp測試程式

# COMMAND ----------

# dw2c_table = "auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_test_ods"
# fusion_table = "auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods"
# process_stage = "ARRAY"
# START_UPDATE_DTM = '2024-12-14 00:00:00'  
# END_UPDATE_DTM = '2024-12-15 00:00:00'
# # PK_COLS_LIST = ['S.PROCESS_STAGE','S.FAB_CODE','S.TFT_CHIP_ID_RRN','S.TEST_TYPE','S.TEST_TIME']
# PK_COLS_LIST = ['PROCESS_STAGE','FAB_CODE','TFT_CHIP_ID_RRN','TEST_TYPE','S.TEST_TIME']

# if process_stage == "ARRAY":
#   sqlcond_ps = ["ARRAY"]
# elif process_stage == "CELL":
#   sqlcond_ps = ["BEOL", "FEOL", "CELL"]

# formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)

# sql_check_str = f"""
# --SELECT S.FAB_CODE, S.PROCESS_STAGE, S.TFT_CHIP_ID_RRN, S.TEST_TYPE, S.TEST_TIME, S.FIRST_YIELD_FLAG, S.FINAL_YIELD_FLAG, S.DEFECT_CODE, S.GRADE, S.GRADE_CHANGE_FLAG
# --,T.FAB_CODE, T.PROCESS_STAGE, T.TFT_CHIP_ID_RRN, T.TEST_TYPE, T.TEST_TIME, T.FIRST_YIELD_FLAG, T.FINAL_YIELD_FLAG, T.DEFECT_CODE, T.GRADE, T.GRADE_CHANGE_FLAG     
# SELECT *
# FROM {fusion_table} T
# RIGHT JOIN 
# (SELECT * FROM  {dw2c_table} 
# --WHERE UPDATE_DTM >= '{START_UPDATE_DTM}'  AND UPDATE_DTM < '{END_UPDATE_DTM}'
# WHERE TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' AND FAB_CODE = '101860'
# )  S 
# ON S.fab_code=T.fab_code 
# and S.PROCESS_STAGE = T.PROCESS_STAGE 
# and S.TFT_CHIP_ID_RRN=T.TFT_CHIP_ID_RRN 
# and ((S.TEST_TYPE = T.TEST_TYPE) OR (S.TEST_TYPE = '-' AND T.TEST_TYPE IS NULL) ) 
# and S.TEST_TIME=T.TEST_TIME
# WHERE TRIM(S.PROCESS_STAGE) IN ({formatted_filter_ps_values})
# AND S.FAB_CODE = '101860'
# --AND T.UPDATE_DTM >= '2024-12-10 09:00:00'  
# --AND T.UPDATE_DTM < '2024-12-10 10:00:00' 

# """
# print(f"Check SQL:{sql_check_str}")

# df = spark.sql(sql_check_str)
# df.cache()
# df.display()

# check_df = df.transform(check_cols)
# check_df.cache()
# check_df.display()
 

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
  CHECK_TYPE = dbutils.widgets.get("CHECK_TYPE")
except:
  if IS_TEST:
    START_TIME = '2025-02-11 00:00:00'
    END_TIME = '2025-02-11 23:59:59'
    CHECK_TYPE = 'BRISA'
    
if START_TIME == '' and END_TIME == '':
  now = datetime.now(TIME_ZONE) - timedelta(days=2) 
  new_time = now - timedelta(minutes=60)

  rounded_now = round_time(now, 60)  
  rounded_new_time = round_time(new_time, 60)  
    
  START_TIME = rounded_new_time.strftime('%Y-%m-%d 00:00:00')  
  END_TIME = rounded_now.strftime('%Y-%m-%d 23:59:59')  

print(f"START_TIME = {START_TIME}")
print(f"END_TIME = {END_TIME}")

# COMMAND ----------

PK_COLS_LIST = ['FAB_CODE', 'PROCESS_STAGE', 'TFT_CHIP_ID_RRN', 'TEST_TYPE', 'TEST_TIME']

JOIN_STR = """
  T.FAB_CODE = S.FAB_CODE
  and T.PROCESS_STAGE = S.PROCESS_STAGE
  and T.TEST_TYPE = S.TEST_TYPE
  and T.TEST_TIME = S.TEST_TIME
  and CAST(T.TFT_CHIP_ID_RRN AS STRING) = CAST(S.TFT_CHIP_ID_RRN AS STRING)
"""

# COMMAND ----------

def get_check_cols():
  _dict = [
        # "PROCESS_STAGE",
        # "FAB_CODE",
        # "CREATE_DTM",
        "SITE_TYPE",
        "SITE_ID",
        "TOOL_ID",
        "OP_ID",
        "SHIFT",
        "ARRAY_LOT_ID",
        "TFT_GLASS_ID",
        "TFT_SUB_SHEET_ID",
        "TFT_CHIP_ID",
        "PRODUCT_CODE",
        "ABBR_NO",
        # "MODEL_NO",
        "PART_NO",
        # "TEST_TIME",
        "TEST_USER",
        # "TEST_TYPE",
        "TEST_MODE",
        "MFG_DAY",
        # "JUDGE_FLAG",
        # "PRE_GRADE",
        "GRADE",
        # "GRADE_CHANGE_FLAG",
        # "JUDGE_CNT",
        "DEFECT_TYPE",
        # "DEFECT_CODE",
        # "DEFECT_CODE_DESC",
        # "DEFECT_VALUE",
        "NOTE_FLAG",
        "NOTE",
        # "UPDATE_DTM",
        # "FIRST_YIELD_FLAG",
        # "FINAL_YIELD_FLAG",
        "CHIP_TYPE",
        # "PRE_DEFECT_CODE",
        # "PRE_DEFECT_CODE_DESC",
        # "TFT_CHIP_ID_RRN",
        "CUR_LOT_ID",
        "AUO_CHIP_ID",
        "OP_CAT",
        # "NEXT_OP_ID",
        # "IS_OUTPUT_NG",
        "PNL_REPR_CNT",
        "INPUT_PART_NO",
        "DISASSEMBLY_TIME",
        "LVL",
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

def transform_cols(df):
  return (
    df
    .withColumn("TEST_TYPE", F.when(F.col("TEST_TYPE").isNull(), "").otherwise(F.regexp_replace("TEST_TYPE", "-", "")))
    # .withColumn("DEFECT_CODE_DESC", F.when(F.col("DEFECT_CODE_DESC").isNull(), "").otherwise(F.regexp_replace("DEFECT_CODE_DESC", "-", "")))
    # .withColumn("DEFECT_CODE", F.when(F.col("DEFECT_CODE").isNull(), "").otherwise(F.regexp_replace("DEFECT_CODE", "-", "")))
    .withColumn("NOTE", F.when(F.col("NOTE").isNull(), "").otherwise(F.regexp_replace("NOTE", "-", "")))
    # .withColumn("PRE_DEFECT_CODE_DESC", F.when(F.col("PRE_DEFECT_CODE_DESC").isNull(), "").otherwise(F.regexp_replace("PRE_DEFECT_CODE_DESC", "-", "")))
  )

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
  elif check_type == 'LCM_BEOL':
    return df.filter(f"PROCESS_STAGE in ('BEOL') AND FAB_CODE IN (115000, 115060)")
  elif check_type == 'EDI':
    return df.filter(f"FAB_CODE IN (117118, 117027, 117511, 117830, 116500, 117114, 117385, 117133, 117021, 189001, 189002, 117540, 117475, 127100)")
  elif check_type == 'EDI_BEOL':
    return df.filter(f"PROCESS_STAGE in ('BEOL') AND FAB_CODE IN (117114, 117118)")
  elif check_type == 'BRISA':
    return df.filter(f"FAB_CODE IN (115400, 117013, 117381, 117130, 117912, 117926, 102455, 117133, 117021, 117330, 117410, 117480, 117520, 117940, 117918, 117920, 117919)")
  else:
    return df.filter(f"""PROCESS_STAGE NOT IN ('ARRAY', 'FEOL', 'BEOL') AND FAB_CODE NOT IN (115400, 117013, 117381, 117130, 117912, 117926, 102455, 117133, 117021, 117330, 117410, 117480, 117520, 117940, 117918, 117920, 117919, 117118, 117027, 117511, 117830, 116500, 117114, 117385, 117133, 117021, 189001, 189002, 117540, 117475, 127100) AND (PROCESS_STAGE in ('MODULE') AND FAB_CODE NOT IN (105000, 105060, 105160, 115000, 117011, 115060, 115100, 115200, 115160, 117006, 111690, 101700)) AND FAB_CODE NOT IN (115270)""")



# COMMAND ----------

def get_sorce_dataframe(check_type):
  return spark.table("auofab.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods").transform(filter_time).transform(filter_process_stage_and_fab, check_type).transform(select_stage_cols, is_source=True).transform(transform_cols)
  
def get_target_dataframe(check_type):
  return spark.table("auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_test_ods").transform(filter_time).transform(filter_process_stage_and_fab, check_type).transform(select_stage_cols, is_source=False).transform(transform_cols)

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def __init__(self, *args, **kwargs):  
        super(TestHelpers, self).__init__(*args, **kwargs)  

    def setUp(self):  
      self.sorce_df = get_sorce_dataframe(CHECK_TYPE)  
      self.target_df = get_target_dataframe(CHECK_TYPE)
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

      for fab_code, site_id in fab_codes:
        print(f"FAB_CODE: {fab_code}, SITE_ID: {site_id}")
        fab_df = check_df.filter((F.col("T.FAB_CODE") == fab_code) & (F.col("T.SITE_ID") == site_id))
        fab_df.display()
      # fab_df = check_df.filter(f"T.MFG_FAB_CODE == '111690'")
      # fab_df.display()

# COMMAND ----------

# if CHECK_TYPE == 'ARRAY':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'FEOL':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'BEOL':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'MODULE':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'LCM_BEOL':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'EDI':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'EDI_BEOL':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'BRISA':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# if CHECK_TYPE == 'OTHER':
#   r = unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

IS_SEND_MAIL = False
CHECK_TYPE_LIST = {
  'ARRAY': None, 
  'FEOL': None,
  'BEOL': None,
  'MODULE': None,
  'LCM_BEOL': None, 
  'EDI_BEOL': None,
  'EDI': None,
  'BRISA': None,
  'OTHER': None,
}

def check_data(check_type: str):
  sorce_df = get_sorce_dataframe(check_type)  
  target_df = get_target_dataframe(check_type)

  check_df = (
    target_df.alias('T')
    .join(sorce_df.alias('S'), on=F.expr(JOIN_STR), how="left")
    .transform(check_cols)
  )
  check_df.cache()
  check_df.display()

  # 取得 NG 欄位
  stage_cols_lists = get_check_cols()
  for col in stage_cols_lists:
    check_df = check_df.withColumn(f"{col}_Flag_C", F.when(F.col(f"{col}_Flag_C") == 'NG', F.lit(col)).otherwise(None))

  check_df = (
    check_df
    .withColumn("NG_COLS", F.array(*[f"{col}_Flag_C" for col in stage_cols_lists]))
    .withColumn("NG_COLS", F.array_compact("NG_COLS"))
  )
  # result_df.display()

  # sand_mail
  check_all_col_df = check_df.groupby("T.FAB_CODE", "T.SITE_ID", "T.PROCESS_STAGE", "NG_COLS").count()
  # check_all_col_df.display()
  check_all_col_html = check_all_col_df.toPandas().to_html()

  return check_all_col_html, check_all_col_df.count() > 0

for check_type in CHECK_TYPE_LIST.keys():
  print(f"START: {check_type}")
  CHECK_TYPE_LIST[check_type], temp_send_mail = check_data(check_type)
  IS_SEND_MAIL = IS_SEND_MAIL or temp_send_mail 

# COMMAND ----------

if IS_SEND_MAIL:
  send_mail(
    data_mail = f"""<B><font color='blue'>ARRAY</B><br/>{CHECK_TYPE_LIST['ARRAY']}</font><br/><B><font color='blue'>FEOL</B><br/>{CHECK_TYPE_LIST['FEOL']}<br/><B><font color='blue'>BEOL</B><br/>{CHECK_TYPE_LIST['BEOL']}<br/><B><font color='blue'>MODULE</B><br/>{CHECK_TYPE_LIST['MODULE']}<br/><B><font color='blue'>LCM_BEOL</B><br/>{CHECK_TYPE_LIST['LCM_BEOL']}<br/><B><font color='blue'>EDI_BEOL</B><br/>{CHECK_TYPE_LIST['EDI_BEOL']}<br/><B><font color='blue'>EDI</B><br/>{CHECK_TYPE_LIST['EDI']}<br/><B><font color='blue'>BRISA</B><br/>{CHECK_TYPE_LIST['BRISA']}<br/><B><font color='blue'>OTHER</B><br/>{CHECK_TYPE_LIST['OTHER']}<br/>
    """,
    send_from = f"FBK Test 資料比對結果",
    send_to = ['Cory.CC.Cheng@auo.com', 'Perry.Chien@auo.com', 'steven.Yeh@auo.com'],
    send_subject = f"FBK Test 驗證不一致: START_TIME = {START_TIME}, END_TIME = {END_TIME}"
  )
else:
  print(f"資料比對一致")
