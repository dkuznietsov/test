# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 檢查目的端table資料是否都有在來源資料(未驗證資料欄未)

# COMMAND ----------

# DBTITLE 1,config
process_stage = "ARRAY" # ARRAY CELL MODULE CF

if process_stage == "ARRAY":
  sqlcond_ps = ["ARRAY"]
  fab_code_dict = {
  "L3C": 101100,
  "L3D": 101160,
  "L4A": 101200,
  # "L4B": 161210,
  # "L5A": 101300,
  "L5B": 101360,
  "L5C": 101500,
  "L5D": 101560,
  "L6A": 101600,
  "L6B": 101660,
  "L6K": 111690,
  "L7A": 101700,
  "L7B": 101760,
  "L8A": 101800,
  "L8B": 101860
  }
elif process_stage == "CELL":
  sqlcond_ps = ["BEOL", "FEOL", "CELL"]
  fab_code_dict = {
  "L3C": 101100,
  # "L3D": 101160,
  # "L4A": 101200,
  # "L4B": 161210,
  # "L5A": 101300,
  "L5B": 101360,
  "L5C": 101500,
  "L5D": 101560,
  "L6A": 101600,
  "L6B": 101660,
  "L6K": 111690,
  "L7A": 101700,
  # "L7B": 101760,
  "L8A": 101800,
  "L8B": 101860
  }  
# 串成in sql 條件
formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)

START_UPDATE_DTM = '2024-12-05 06:15:00'  
END_UPDATE_DTM = '2024-12-14 07:45:00'

# COMMAND ----------

# DBTITLE 1,比對 sql
table_tag = f"h_dax_fbk_fma_ods"

#來源表sql 
sql_template = """

SELECT CREATE_DTM, UPDATE_DTM, TFT_CHIP_ID_RRN, SITE_TYPE, SITE_ID, TOOL_ID, OP_ID, SHIFT, ARRAY_LOT_ID, TFT_GLASS_ID, TFT_SUB_SHEET_ID, CHIP_TYPE, TFT_CHIP_ID, CF_GLASS_ID, CF_SUB_SHEET_ID, CF_CHIP_ID, PRODUCT_CODE, ABBR_NO, MODEL_NO, PART_NO, TEST_TIME, FMA_TIME, FMA_USER, TEST_TYPE, MFG_DAY, JUDGE_FLAG, TEST_GRADE, FMA_GRADE, GRADE_CHANGE_FLAG,JUDGE_CNT, TEST_JUDGE_CNT, DEFECT_TYPE, DEFECT_CODE, DEFECT_CODE_DESC, DEFECT_VALUE, DEFECT_AREA, DEFECT_LOCATION, FMA_DEFECT_CODE, FMA_DEFECT_CODE_DESC, F_CLASS, SIGNAL_NO, GATE_NO, POX_X, POX_Y, POSITION, LOCATION_FLAG, NOTE_FLAG, NOTE, FMA_CUT_FLAG, PROCESS_STAGE, FAB_CODE
FROM
( 
SELECT ROW_NUMBER() OVER(PARTITION BY FAB_CODE, PROCESS_STAGE, TFT_CHIP_ID_RRN, TEST_TYPE, TEST_TIME ORDER BY UPDATE_DTM DESC) AS INDEX,*    
FROM auofab.itgp_eng.{key}_{process_stage}_{table_tag}
WHERE FAB_CODE = '{value}'  
      AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values})
      AND UPDATE_DTM >= '{START_UPDATE_DTM}'  
      AND UPDATE_DTM < '{END_UPDATE_DTM}'
)
WHERE INDEX=1
"""
# 生成 SQL 查询字符串
query_list = []
for key, value in fab_code_dict.items():
    query = sql_template.format(key=key, value=value,process_stage=process_stage.lower(), START_UPDATE_DTM=START_UPDATE_DTM, END_UPDATE_DTM=END_UPDATE_DTM, formatted_filter_ps_values=formatted_filter_ps_values,table_tag=table_tag)
    query_list.append(query)

sql_source_str = ' UNION '.join(f"{value}" for value in query_list) 

sql_fusion_str = f"""
SELECT *     
FROM auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_{table_tag}
WHERE TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values})
"""

print(f"Source SQL:{sql_source_str}")
print(f"Fusion SQL:{sql_fusion_str}")


# COMMAND ----------

# DBTITLE 1,驗證資料
PK_COLS_LIST = ['PROCESS_STAGE','FAB_CODE','TFT_CHIP_ID_RRN','JUDGE_CNT','FMA_TIME']
df_source = spark.sql(sql_source_str)
df_fusion = spark.sql(sql_fusion_str)

print("Source df")
df_source.display()
print("Fusion df")
df_fusion.display()

# 已來源資料為主
df_merged = df_source.join(df_fusion, on=PK_COLS_LIST, how='left')

# 列出 df_source 中存在而不在 df_fusion 中的資料
df_result = df_merged.filter(df_fusion[PK_COLS_LIST[0]].isNull())  # 检查 df_fusion 的某列是否为空

count = df_result.count()
if count > 0:
  df_result.display()
  print(f"process_stage={process_stage},time:{START_UPDATE_DTM} - {END_UPDATE_DTM}, 不一致筆數, count={df_result.count()}")
else:
  print(f"process_stage={process_stage},time:{START_UPDATE_DTM} - {END_UPDATE_DTM}, 沒有不一致")

# COMMAND ----------

# MAGIC %md
# MAGIC #比對地上雲資料表與fusion資料表
# MAGIC auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_test_ods、auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods

# COMMAND ----------

# DBTITLE 1,check function
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType,DateType, DecimalType, ShortType

def check_cols(df):
  #比對欄位
  stage_cols_lists = []

  selected_columns = []
  # col_exprs = []  
  # for col in stage_cols_lists:
  #   col_expr = F.when((F.col(f"S.{col}") == 0) & (F.col(f"T.{col}").isNull()), "NULL").when(((F.col(f"S.{col}") != 0) & (F.col(f"T.{col}").isNull())) | (F.round(F.col(f"S.{col}")) != F.round(F.col(f"T.{col}"))), "NG").otherwise(f"OK").alias(f"{col}_Flag")
  #   # col_exprs.append(col_expr) 
  #   selected_columns.extend([F.col(f"S.{col}"), F.col(f"T.{col}"), col_expr])

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
  
  # print(selected_columns)

  filter_condition = ' OR '.join([f"{col}_Flag like 'NG' " for col in stage_cols_lists])
  # if stage != 'MODULE':
  #   return df.select(*PK_COLS_LIST + [f'S.{get_stage_part_cols_str(stage)}', f'T.{get_stage_part_cols_str(stage)}'] + selected_columns).where(filter_condition) 
  # else:  
  return df.select(*PK_COLS_LIST + selected_columns).where(filter_condition) 


# COMMAND ----------

# DBTITLE 1,比對資料
table_tag = f"h_dax_fbk_pcccode_ods"
process_stage = "CELL" # ARRAY CELL
START_UPDATE_DTM = '2024-12-15 21:15:00'  
END_UPDATE_DTM = '2024-12-15 23:45:00'
PK_COLS_LIST = ['PROCESS_STAGE','FAB_CODE','TFT_CHIP_ID_RRN','PCC_CODE']

if process_stage == "ARRAY":
  sqlcond_ps = ["ARRAY"]
  master_fabcode_col = "FEOL_FAB_CODE"
  fab_code_dict = {
  "L3C": 101100,
  "L3D": 101160,
  "L4A": 101200,
  # "L4B": 161210,
  # "L5A": 101300,
  "L5B": 101360,
  "L5C": 101500,
  "L5D": 101560,
  "L6A": 101600,
  "L6B": 101660,
  "L6K": 111690,
  "L7A": 101700,
  "L7B": 101760,
  "L8A": 101800,
  "L8B": 101860
  }
elif process_stage == "CELL":
  sqlcond_ps = ["BEOL", "FEOL", "CELL"]
  master_fabcode_col = "MODULE_FAB_CODE"
  fab_code_dict = {
  "L3C": 101100,
  # "L3D": 101160,
  # "L4A": 101200,
  # "L4B": 161210,
  # "L5A": 101300,
  "L5B": 101360,
  "L5C": 101500,
  "L5D": 101560,
  "L6A": 101600,
  "L6B": 101660,
  "L6K": 111690,
  "L7A": 101700,
  # "L7B": 101760,
  "L8A": 101800,
  "L8B": 101860
  } 

formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)
formatted_filter_fabcode_values = ', '.join(str(value) for value in fab_code_dict.values())

sql_source_str = f"""
SELECT * 
FROM auofab.prod_dw.dt_all_s_all_dw_at_feedback_{table_tag}
WHERE 1=1 
      --AND FAB_CODE = '101860' 
      AND FAB_CODE in ({formatted_filter_fabcode_values})
      AND TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' 
      AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values}) 
"""

sql_fusion_str = f"""
SELECT * 
FROM auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_{table_tag}
WHERE 1=1 
      --AND FAB_CODE = '101860' 
      AND FAB_CODE in ({formatted_filter_fabcode_values})
      AND TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' 
      AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values}) 
"""
print(f"Source SQL:{sql_source_str}")
print(f"Fusion SQL:{sql_fusion_str}")

#merge key NULL補-
df_source = spark.sql(sql_source_str).withColumn("PCC_CODE", F.when(
                F.col("PCC_CODE").isNull(),
                F.lit('-')).otherwise(F.col("PCC_CODE")))
df_fusion = spark.sql(sql_fusion_str)
# df_source.cache()
# df_fusion.cache()

print(f"Source df count={df_source.count()}")
df_source.display()
print(f"Fusion df count={df_fusion.count()}")
df_fusion.display()


df = df_source.alias('S').join(df_fusion.alias('T'), on=PK_COLS_LIST , how='left')
check_df = df.transform(check_cols)
# check_df.cache()
print(f"{process_stage} 資料比對，不一致count={check_df.count()}")
check_df.display()

filter_fab_code_str = "fab_code not in (111690) "
check_df = check_df.filter(f"{filter_fab_code_str}")
print(f"{process_stage} 資料比對(過濾L6K)，不一致 count={check_df.count()}")
check_df.display()
 

# COMMAND ----------

# MAGIC %md
# MAGIC #以下為Temp測試程式

# COMMAND ----------

dw2c_table = "auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_test_ods"
fusion_table = "auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods"
process_stage = "ARRAY"
START_UPDATE_DTM = '2024-12-14 00:00:00'  
END_UPDATE_DTM = '2024-12-15 00:00:00'
# PK_COLS_LIST = ['S.PROCESS_STAGE','S.FAB_CODE','S.TFT_CHIP_ID_RRN','S.TEST_TYPE','S.TEST_TIME']
PK_COLS_LIST = ['PROCESS_STAGE','FAB_CODE','TFT_CHIP_ID_RRN','TEST_TYPE','S.TEST_TIME']

if process_stage == "ARRAY":
  sqlcond_ps = ["ARRAY"]
elif process_stage == "CELL":
  sqlcond_ps = ["BEOL", "FEOL", "CELL"]

formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)

sql_check_str = f"""
--SELECT S.FAB_CODE, S.PROCESS_STAGE, S.TFT_CHIP_ID_RRN, S.TEST_TYPE, S.TEST_TIME, S.FIRST_YIELD_FLAG, S.FINAL_YIELD_FLAG, S.DEFECT_CODE, S.GRADE, S.GRADE_CHANGE_FLAG
--,T.FAB_CODE, T.PROCESS_STAGE, T.TFT_CHIP_ID_RRN, T.TEST_TYPE, T.TEST_TIME, T.FIRST_YIELD_FLAG, T.FINAL_YIELD_FLAG, T.DEFECT_CODE, T.GRADE, T.GRADE_CHANGE_FLAG     
SELECT *
FROM {fusion_table} T
RIGHT JOIN 
(SELECT * FROM  {dw2c_table} 
--WHERE UPDATE_DTM >= '{START_UPDATE_DTM}'  AND UPDATE_DTM < '{END_UPDATE_DTM}'
WHERE TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' AND FAB_CODE = '101860'
)  S 
ON S.fab_code=T.fab_code 
and S.PROCESS_STAGE = T.PROCESS_STAGE 
and S.TFT_CHIP_ID_RRN=T.TFT_CHIP_ID_RRN 
and ((S.TEST_TYPE = T.TEST_TYPE) OR (S.TEST_TYPE = '-' AND T.TEST_TYPE IS NULL) ) 
and S.TEST_TIME=T.TEST_TIME
WHERE TRIM(S.PROCESS_STAGE) IN ({formatted_filter_ps_values})
AND S.FAB_CODE = '101860'
--AND T.UPDATE_DTM >= '2024-12-10 09:00:00'  
--AND T.UPDATE_DTM < '2024-12-10 10:00:00' 

"""
print(f"Check SQL:{sql_check_str}")

df = spark.sql(sql_check_str)
df.cache()
df.display()

check_df = df.transform(check_cols)
check_df.cache()
check_df.display()
 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auofab.itgp_eng.l8B_array_h_dax_fbk_test_ods T 
# MAGIC WHERE TRIM(T.PROCESS_STAGE) IN ('ARRAY')
# MAGIC AND T.UPDATE_DTM >= '2024-12-10 09:00:00'  
# MAGIC AND T.UPDATE_DTM < '2024-12-10 10:00:00' 
# MAGIC AND TFT_CHIP_ID_RRN IN ('104660396432951')
# MAGIC --AND ARRAY_LOT_ID IN ('ZH4KC6W200')
# MAGIC AND TEST_TYPE='LASER'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auofabDEV.prod_dw.MF_all_s_all_dw_at_feedback_h_dax_fbk_test_ods T 
# MAGIC
# MAGIC WHERE TRIM(T.PROCESS_STAGE) IN ('ARRAY')
# MAGIC AND T.FAB_CODE = '101860' 
# MAGIC --AND ARRAY_LOT_ID IN ('ZH4KC6W200')
# MAGIC AND TFT_CHIP_ID_RRN IN ('104660396432951')
# MAGIC AND TEST_TYPE='LASER'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auofab.prod_dw.DT_all_s_all_dw_at_feedback_h_dax_fbk_test_ods T 
# MAGIC
# MAGIC WHERE TRIM(T.PROCESS_STAGE) IN ('ARRAY')
# MAGIC --AND T.UPDATE_DTM >= '2024-12-10 09:00:00'  
# MAGIC --AND T.UPDATE_DTM < '2024-12-10 10:00:00' 
# MAGIC AND TFT_CHIP_ID_RRN IN ('104660396432951')
# MAGIC AND TEST_TIME= '2023-12-16 18:24:32'
# MAGIC --AND ARRAY_LOT_ID IN ('ZH4KC6W200')
# MAGIC AND TEST_TYPE='LASER'

# COMMAND ----------

# sql_source_str = f"""
# SELECT * 
# FROM auofab.prod_dw.dt_all_s_all_dw_at_feedback_{table_tag}
# WHERE TFT_CHIP_ID_RRN IN
#       (
#       SELECT TFT_CHIP_ID_RRN
#       FROM auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_master_ods
#       WHERE FEOL_FAB_CODE IS NOT NULL
#             AND TFT_CHIP_ID_RRN  IN 
#             (
#             SELECT TFT_CHIP_ID_RRN 
#             FROM auofab.prod_dw.dt_all_s_all_dw_at_feedback_{table_tag}
#             WHERE 1=1 
#                   --AND FAB_CODE = '101860' 
#                   AND TEST_TIME >= '{START_UPDATE_DTM}'  AND TEST_TIME < '{END_UPDATE_DTM}' 
#                   AND TRIM(PROCESS_STAGE) IN ({formatted_filter_ps_values}) 
#             )
#       )

# """
