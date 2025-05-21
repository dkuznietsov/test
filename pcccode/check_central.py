# Databricks notebook source
# DBTITLE 1,config
process_stage = "CELL" # ARRAY CELL MODULE CF
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
  # "L6K": 111690,
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
  # "L6K": 111690,
  "L7A": 101700,
  # "L7B": 101760,
  "L8A": 101800,
  "L8B": 101860
  }  
# 串成in sql 條件
formatted_filter_ps_values = ', '.join(f"'{value}'" for value in sqlcond_ps)
START_UPDATE_DTM = '2024-12-07 16:00:00'  
END_UPDATE_DTM = '2024-12-07 19:00:00'


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,比對 sql
table_tag = f"h_dax_fbk_pcccode_ods"
#來源表sql 
sql_template = """
SELECT
    CREATE_DTM, 
    UPDATE_DTM,
    TFT_CHIP_ID_RRN,
    CHIP_ID,
    ARRAY_LOT_ID,
    PCC_CODE,
    PCC_REASON_CODE,
    SHEET_ID,
    X_AXIS,
    Y_AXIS,
    DEF_DESC,
    FAB_CODE,
    PROCESS_STAGE,
    OP_ID
FROM
( 
SELECT ROW_NUMBER() OVER(PARTITION BY FAB_CODE, PROCESS_STAGE, TFT_CHIP_ID_RRN, PCC_CODE ORDER BY UPDATE_DTM DESC) AS INDEX,*    
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



# COMMAND ----------

# DBTITLE 1,驗證資料
PK_COLS_LIST = [
    'PROCESS_STAGE',
    'FAB_CODE',
    'TFT_CHIP_ID_RRN',
    'PCC_CODE'
]
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

