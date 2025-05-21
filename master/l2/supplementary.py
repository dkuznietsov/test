# Databricks notebook source
import os
import json
import requests

from typing import Dict
from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType, DateType, IntegerType, DecimalType
from pyspark.sql.utils import AnalysisException

from master.config import Config
from util.deltatable import DeltaTableCRUD
from util.util import spark

# COMMAND ----------

IS_DETAIL = False

config = Config()
delta_schema = config.delta_schema
print(delta_schema)
save_path = config.save_path
print(save_path)
jscs_db_schema = config.jscs_db_schema
print(jscs_db_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # mf_all_s_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp

# COMMAND ----------

_schema = StructType([
    StructField("CREATE_DATE", DateType(), True),
    StructField("CREATE_DTM", TimestampType(), True),
    StructField("TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    StructField("ARRAY_SITE_ID", StringType(), True),
    StructField("ARRAY_FAB_CODE", DecimalType(6,0), True),
    StructField("ARRAY_CHIP_TYPE", StringType(), True),
    StructField("ARRAY_WO_ID", StringType(), True),
    StructField("ARRAY_LOT_ID", StringType(), True),
    StructField("TFT_GLASS_ID", StringType(), True),
    StructField("TFT_CHIP_ID", StringType(), True),
    StructField("ARRAY_PRODUCT_CODE", StringType(), True),
    StructField("ARRAY_ABBR_NO", StringType(), True),
    StructField("ARRAY_MODEL_NO", StringType(), True),
    StructField("ARRAY_PART_NO", StringType(), True),
    StructField("ARRAY_START_DATE", TimestampType(), True),
    StructField("ARRAY_FINAL_GRADE", StringType(), True),
    StructField("ARRAY_COMP_DTM", TimestampType(), True),
    StructField("ARRAY_SCRAP_FLAG", StringType(), True),
    StructField("ARRAY_SCRAP_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_SHIPPING_FLAG", StringType(), True),
    StructField("ARRAY_SHIPPING_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_TERMINATE_FLAG", StringType(), True),
    StructField("ARRAY_TERMINATE_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_RECYCLE_FLAG", StringType(), True),
    StructField("ARRAY_RECYCLE_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_INPUT_CANCEL_FLAG", StringType(), True),
    StructField("ARRAY_INPUT_CANCEL_FLAG_DTM", TimestampType(), True),
    StructField("CURRENT_STAGE", StringType(), True),
    StructField("TFT_SUB_SHEET_ID", StringType(), True),
    StructField("EVENT_DTM", TimestampType(), True)
])

_df = spark.createDataFrame(data=[], schema=_schema)
_df.printSchema()

target_delta_table_location = f"{save_path}/mf_all_b_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp"
tablename = f"{delta_schema}.mf_all_b_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp"
print(target_delta_table_location)
print(tablename)

_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename, path=target_delta_table_location)
# .partitionBy("ARRAY_FAB_CODE").saveAsTable(tablename, path=target_delta_table_location)

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_b_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp CLUSTER BY (ARRAY_FAB_CODE)")
spark.sql(f'ALTER TABLE {delta_schema}.mf_all_b_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)') 

# COMMAND ----------

# MAGIC %md
# MAGIC # mf_all_s_ary_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp

# COMMAND ----------

_schema = StructType([  
    StructField("CREATE_DATE", DateType(), True),
    StructField("CREATE_DTM", TimestampType(), True),
    StructField("MAPPING_TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    # StructField("SOURCE_TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    # StructField("TARGET_TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    StructField("TFT_CHIP_ID_RRN", DecimalType(30,0), True),  
    StructField("FEOL_FAB_CODE", DecimalType(6,0), True), 
    StructField("ARRAY_LOT_ID", StringType(), True),  
    # ----
    StructField("ARRAY_SITE_ID", StringType(), True),
    StructField("ARRAY_FAB_CODE", DecimalType(6,0), True),
    StructField("ARRAY_CHIP_TYPE", StringType(), True),
    StructField("ARRAY_WO_ID", StringType(), True),
    StructField("ARRAY_PRODUCT_CODE", StringType(), True),
    StructField("ARRAY_ABBR_NO", StringType(), True),
    StructField("ARRAY_MODEL_NO", StringType(), True),
    StructField("ARRAY_PART_NO", StringType(), True),
    StructField("ARRAY_1ST_GRADE", StringType(), True),
    StructField("ARRAY_1ST_TEST_TIME", TimestampType(), True),
    StructField("ARRAY_START_DATE", TimestampType(), True),
    StructField("ARRAY_FINAL_GRADE", StringType(), True),
    StructField("ARRAY_FINAL_TEST_TIME", TimestampType(), True),
    StructField("ARRAY_COMP_DTM", TimestampType(), True),
    StructField("ARRAY_SCRAP_FLAG", StringType(), True),
    StructField("ARRAY_SCRAP_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_SHIPPING_FLAG", StringType(), True),
    StructField("ARRAY_SHIPPING_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_TERMINATE_FLAG", StringType(), True),
    StructField("ARRAY_TERMINATE_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_RECYCLE_FLAG", StringType(), True),
    StructField("ARRAY_RECYCLE_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_INPUT_CANCEL_FLAG", StringType(), True),
    StructField("ARRAY_INPUT_CANCEL_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_UPDATE_DTM", TimestampType(), True),
    StructField("ARRAY_WH_SHIPPING_DTM", TimestampType(), True),
    StructField("ARRAY_INPUT_PART_NO", StringType(), True),
    # ----
    StructField("FEOL_SITE_ID", StringType(), True),  
     
    StructField("FEOL_CHIP_TYPE", StringType(), True),  
    StructField("FEOL_WO_ID", StringType(), True),  
    StructField("FEOL_BATCH_ID", StringType(), True),  
    StructField("TFT_GLASS_ID", StringType(), True),  
    StructField("TFT_SUB_SHEET_ID", StringType(), True),  
    StructField("TFT_CHIP_ID", StringType(), True),  
    StructField("CF_SITE_ID", StringType(), True),  
    StructField("CF_FAB_CODE", DecimalType(6,0), True),  
    StructField("CF_LOT_ID", StringType(), True),  
    StructField("CF_GLASS_ID", StringType(), True),  
    StructField("CF_SUB_SHEET_ID", StringType(), True),  
    StructField("CF_CHIP_ID", StringType(), True),  
    StructField("CF_WO_ID", StringType(), True),  
    StructField("CF_PRODUCT_CODE", StringType(), True),  
    StructField("CF_ABBR_NO", StringType(), True),  
    StructField("CF_MODEL_NO", StringType(), True),  
    StructField("CF_PART_NO", StringType(), True),  
    StructField("CF_FINAL_GRADE", StringType(), True),  
    StructField("CF_SHIPPING_FLAG", StringType(), True),  
    StructField("CF_SHIPPING_FLAG_DTM", TimestampType(), True),  
    StructField("FEOL_PRODUCT_CODE", StringType(), True),  
    StructField("FEOL_ABBR_NO", StringType(), True),  
    StructField("FEOL_MODEL_NO", StringType(), True),  
    StructField("FEOL_PART_NO", StringType(), True),  
    StructField("FEOL_START_DATE", TimestampType(), True),  
    StructField("CELL_PROCESS_TYPE", StringType(), True),  
    StructField("FEOL_SHIPPING_FLAG", StringType(), True),  
    StructField("FEOL_SHIPPING_FLAG_DTM", TimestampType(), True),  
    StructField("CELL_SCRAP_FLAG", StringType(), True),  
    StructField("CELL_SCRAP_FLAG_DTM", TimestampType(), True),  
    StructField("FEOL_RECEIVE_DTM", TimestampType(), True),  
    StructField("CURRENT_STAGE", StringType(), True),  
    StructField("EVENT_DTM", TimestampType(), True),
    StructField("PRT_FAB_CODE", DecimalType(6,0), True),  
    StructField("PRT_WO_ID", StringType(), True),  
    StructField("PRT_PRODUCT_CODE", StringType(), True),  
    StructField("PRT_MODEL_NO", StringType(), True),  
    StructField("PRT_ABBR_NO", StringType(), True),  
    StructField("PRT_LOT_ID", StringType(), True),  
    StructField("PRT_GLASS_ID", StringType(), True),  
    StructField("PRT_CHIP_ID", StringType(), True),  
    StructField("PRT_PART_NO", StringType(), True),  
    StructField("PRT_FINAL_GRADE", StringType(), True),  
    StructField("PRT_SHIPPING_FLAG", StringType(), True),  
    StructField("PRT_SHIPPING_FLAG_DTM", TimestampType(), True),  
    StructField("CELL_PRODUCTION_AREA", StringType(), True)
])


_df = spark.createDataFrame(data=[], schema=_schema)
_df.printSchema()

target_delta_table_location = f"{save_path}/mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp"
tablename = f"{delta_schema}.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp"
print(target_delta_table_location)
print(tablename)

_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename, path=target_delta_table_location)

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp CLUSTER BY (FEOL_FAB_CODE)")
spark.sql(f'ALTER TABLE {delta_schema}.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)') 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # mf_all_s_ary_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp

# COMMAND ----------

_schema = StructType([
    StructField("CREATE_DATE", DateType(), True),
    StructField("CREATE_DTM", TimestampType(), True),
    StructField("MAPPING_TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    # StructField("SOURCE_TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    # StructField("TARGET_TFT_CHIP_ID_RRN", DecimalType(30,0), True),
    StructField("TFT_CHIP_ID_RRN", DecimalType(30,0), True), 
    StructField("BEOL_FAB_CODE", DecimalType(6,0), True),  
    StructField("ARRAY_LOT_ID", StringType(), True),
    # ---
    StructField("ARRAY_SITE_ID", StringType(), True),
    StructField("ARRAY_FAB_CODE", DecimalType(6,0), True),
    StructField("ARRAY_CHIP_TYPE", StringType(), True),
    StructField("ARRAY_WO_ID", StringType(), True),
    StructField("ARRAY_PRODUCT_CODE", StringType(), True),
    StructField("ARRAY_ABBR_NO", StringType(), True),
    StructField("ARRAY_MODEL_NO", StringType(), True),
    StructField("ARRAY_PART_NO", StringType(), True),
    StructField("ARRAY_1ST_GRADE", StringType(), True),
    StructField("ARRAY_1ST_TEST_TIME", TimestampType(), True),
    StructField("ARRAY_START_DATE", TimestampType(), True),
    StructField("ARRAY_FINAL_GRADE", StringType(), True),
    StructField("ARRAY_FINAL_TEST_TIME", TimestampType(), True),
    StructField("ARRAY_COMP_DTM", TimestampType(), True),
    StructField("ARRAY_SCRAP_FLAG", StringType(), True),
    StructField("ARRAY_SCRAP_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_SHIPPING_FLAG", StringType(), True),
    StructField("ARRAY_SHIPPING_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_TERMINATE_FLAG", StringType(), True),
    StructField("ARRAY_TERMINATE_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_RECYCLE_FLAG", StringType(), True),
    StructField("ARRAY_RECYCLE_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_INPUT_CANCEL_FLAG", StringType(), True),
    StructField("ARRAY_INPUT_CANCEL_FLAG_DTM", TimestampType(), True),
    StructField("ARRAY_UPDATE_DTM", TimestampType(), True),
    StructField("ARRAY_WH_SHIPPING_DTM", TimestampType(), True),
    StructField("ARRAY_INPUT_PART_NO", StringType(), True),
    StructField("FEOL_UPDATE_DTM", TimestampType(), True), 
    StructField("FEOL_ABBR_NO", StringType(), True), 
    StructField("FEOL_BATCH_ID", StringType(), True),
    StructField("FEOL_CHIP_TYPE", StringType(), True),  
    StructField("FEOL_FAB_CODE", DecimalType(6,0), True),  
    StructField("FEOL_INPUT_PART_NO", StringType(), True),  
    StructField("FEOL_MODEL_NO", StringType(), True), 
    StructField("FEOL_PART_NO", StringType(), True),  
    StructField("FEOL_PRODUCT_CODE", StringType(), True), 
    StructField("FEOL_RECEIVE_DTM", TimestampType(), True), 
    StructField("FEOL_SHIPPING_FLAG", StringType(), True),  
    StructField("FEOL_SHIPPING_FLAG_DTM", TimestampType(), True), 
    StructField("FEOL_SITE_ID", StringType(), True),   
    StructField("FEOL_START_DATE", TimestampType(), True),  
    StructField("FEOL_WH_RECEIVE_DTM", TimestampType(), True),  
    StructField("FEOL_WH_SHIPPING_DTM", TimestampType(), True),  
    StructField("FEOL_WO_ID", StringType(), True),  
    # ---
    StructField("BEOL_SITE_ID", StringType(), True),  
    StructField("BEOL_CHIP_TYPE", StringType(), True),  
    StructField("BEOL_WO_ID", StringType(), True),  
    StructField("BEOL_BATCH_ID", StringType(), True),  
    StructField("TFT_GLASS_ID", StringType(), True),  
    StructField("TFT_SUB_SHEET_ID", StringType(), True),  
    StructField("TFT_CHIP_ID", StringType(), True),  
    StructField("BEOL_PRODUCT_CODE", StringType(), True),  
    StructField("BEOL_ABBR_NO", StringType(), True),  
    StructField("BEOL_MODEL_NO", StringType(), True),  
    StructField("BEOL_PART_NO", StringType(), True),  
    StructField("BEOL_START_DATE", TimestampType(), True),  
    StructField("CELL_FINAL_GRADE", StringType(), True),  
    StructField("CELL_SCRAP_FLAG", StringType(), True),  
    StructField("CELL_SCRAP_FLAG_DTM", TimestampType(), True),  
    StructField("BEOL_SHIPPING_FLAG", StringType(), True),  
    StructField("BEOL_SHIPPING_FLAG_DTM", TimestampType(), True),  
    StructField("BEOL_RECEIVE_DTM", TimestampType(), True),  
 
    StructField("CURRENT_STAGE", StringType(), True),  
    StructField("EVENT_DTM", TimestampType(), True),
    StructField("PRT_FAB_CODE", DecimalType(6,0), True),  
    StructField("PRT_WO_ID", StringType(), True),  
    StructField("PRT_PRODUCT_CODE", StringType(), True),  
    StructField("PRT_MODEL_NO", StringType(), True),  
    StructField("PRT_ABBR_NO", StringType(), True),  
    StructField("PRT_LOT_ID", StringType(), True),  
    StructField("PRT_GLASS_ID", StringType(), True),  
    StructField("PRT_CHIP_ID", StringType(), True),  
    StructField("PRT_PART_NO", StringType(), True),  
    StructField("PRT_FINAL_GRADE", StringType(), True),  
    StructField("PRT_SHIPPING_FLAG", StringType(), True),  
    StructField("PRT_SHIPPING_FLAG_DTM", TimestampType(), True),  
    StructField("CELL_INPUT_PART_NO", StringType(), True),  
    StructField("CELL_SOURCE_FAB_CODE", DecimalType(6,0), True),  
    StructField("CELL_PRODUCTION_AREA", StringType(), True)  
])  

_df = spark.createDataFrame(data=[], schema=_schema)
_df.printSchema()

target_delta_table_location = f"{save_path}/mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp"
tablename = f"{delta_schema}.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp"
print(target_delta_table_location)
print(tablename)

_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename, path=target_delta_table_location)

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp CLUSTER BY (BEOL_FAB_CODE)")
spark.sql(f'ALTER TABLE {delta_schema}.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)') 

# COMMAND ----------

# MAGIC %md
# MAGIC # mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact

# COMMAND ----------

_schema = StructType([
    StructField("CREATE_DTM", TimestampType(), True),
    StructField("TFT_CHIP_ID_RRN", DecimalType(30,0), True),  
    StructField("ARRAY_FAB_CODE", DecimalType(6,0), True),
    StructField("FEOL_FAB_CODE", DecimalType(6,0), True),  
    StructField("BEOL_FAB_CODE", DecimalType(6,0), True),
    StructField("LM_TIME", TimestampType(), True),
])  

_df = spark.createDataFrame(data=[], schema=_schema)
_df.printSchema()

target_delta_table_location = f"{save_path}/mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact"
tablename = f"{delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact"
print(target_delta_table_location)
print(tablename)

_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename, path=target_delta_table_location)

# COMMAND ----------

# spark.sql(f"ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact CLUSTER BY (BEOL_FAB_CODE)")
spark.sql(f'ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)') 

# COMMAND ----------

# MAGIC %md
# MAGIC # mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time

# COMMAND ----------

_schema = StructType([
    StructField("CREATE_DTM", TimestampType(), True),
    StructField("CREATE_DATE", DateType(), True),
    StructField("TFT_CHIP_ID_RRN", DecimalType(30,0), True),  
    StructField("LM_TIME", TimestampType(), True),
])  

_df = spark.createDataFrame(data=[], schema=_schema)
_df.printSchema()

target_delta_table_location = f"{save_path}/mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time"
tablename = f"{delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time"
print(target_delta_table_location)
print(tablename)

_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename, path=target_delta_table_location)

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time CLUSTER BY (CREATE_DATE)")
spark.sql(f'ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)') 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_create_time
# MAGIC SELECT 
# MAGIC CREATE_DTM, 
# MAGIC CAST(CREATE_DTM AS DATE) AS CREATE_DATE, 
# MAGIC CAST(TFT_CHIP_ID_RRN AS decimal(30,0)) AS TFT_CHIP_ID_RR,
# MAGIC from_utc_timestamp(current_timestamp(), "Asia/Taipei") AS LM_TIME
# MAGIC FROM auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_view

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_view AS
# MAGIC SELECT 
# MAGIC   s.ARRAY_FAB_CODE,  
# MAGIC   s.FEOL_FAB_CODE,  
# MAGIC   s.BEOL_FAB_CODE,  
# MAGIC   s.TFT_CHIP_ID_RRN,  
# MAGIC   s.CREATE_DTM,  
# MAGIC   a.ARRAY_SITE_ID,  
# MAGIC   f.CF_SITE_ID,  
# MAGIC   f.FEOL_SITE_ID,  
# MAGIC   b.BEOL_SITE_ID,  
# MAGIC   a.ARRAY_WO_ID,  
# MAGIC   a.ARRAY_LOT_ID,  
# MAGIC   f.CF_WO_ID,  
# MAGIC   f.CF_LOT_ID,  
# MAGIC   f.CF_GLASS_ID,  
# MAGIC   f.CF_SUB_SHEET_ID,  
# MAGIC   f.CF_CHIP_ID,  
# MAGIC   f.FEOL_WO_ID,  
# MAGIC   f.FEOL_BATCH_ID,  
# MAGIC   b.BEOL_WO_ID,  
# MAGIC   b.BEOL_BATCH_ID,  
# MAGIC   a.TFT_GLASS_ID,  
# MAGIC   a.TFT_SUB_SHEET_ID,  
# MAGIC   a.TFT_CHIP_ID,
# MAGIC   a.ARRAY_PRODUCT_CODE,  
# MAGIC   a.ARRAY_ABBR_NO,  
# MAGIC   a.ARRAY_MODEL_NO,  
# MAGIC   a.ARRAY_PART_NO,  
# MAGIC   f.CF_PRODUCT_CODE,  
# MAGIC   f.CF_ABBR_NO,  
# MAGIC   f.CF_MODEL_NO,  
# MAGIC   f.CF_PART_NO,  
# MAGIC   f.FEOL_PRODUCT_CODE,  
# MAGIC   f.FEOL_ABBR_NO,  
# MAGIC   f.FEOL_MODEL_NO,  
# MAGIC   f.FEOL_PART_NO,  
# MAGIC   b.BEOL_PRODUCT_CODE,  
# MAGIC   b.BEOL_ABBR_NO,  
# MAGIC   b.BEOL_MODEL_NO,  
# MAGIC   b.BEOL_PART_NO,  
# MAGIC   a.ARRAY_FINAL_GRADE,  
# MAGIC   a.ARRAY_SCRAP_FLAG,  
# MAGIC   a.ARRAY_SCRAP_FLAG_DTM,  
# MAGIC   a.ARRAY_SHIPPING_FLAG,  
# MAGIC   a.ARRAY_SHIPPING_FLAG_DTM,  
# MAGIC   b.CELL_SCRAP_FLAG,  
# MAGIC   b.CELL_SCRAP_FLAG_DTM,  
# MAGIC   f.FEOL_SHIPPING_FLAG,  
# MAGIC   f.FEOL_SHIPPING_FLAG_DTM
# MAGIC FROM auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_rrn_fact s
# MAGIC LEFT JOIN auofabdev.prod_dw.mf_all_s_ary_kpi_at_feedback_h_dax_fbk_master_ods_array_temp a
# MAGIC   ON s.TFT_CHIP_ID_RRN = a.TFT_CHIP_ID_RRN
# MAGIC   AND s.ARRAY_FAB_CODE = a.ARRAY_FAB_CODE
# MAGIC LEFT JOIN auofabdev.prod_dw.mf_all_s_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp f
# MAGIC   ON s.TFT_CHIP_ID_RRN = f.MAPPING_TFT_CHIP_ID_RRN
# MAGIC   AND s.FEOL_FAB_CODE = f.FEOL_FAB_CODE
# MAGIC LEFT JOIN auofabdev.prod_dw.mf_all_s_cel_kpi_at_feedback_h_dax_fbk_master_ods_beol_temp b
# MAGIC   ON s.TFT_CHIP_ID_RRN = b.MAPPING_TFT_CHIP_ID_RRN
# MAGIC   AND s.BEOL_FAB_CODE = b.BEOL_FAB_CODE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods_view 

# COMMAND ----------

# MAGIC %md
# MAGIC # mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods

# COMMAND ----------

master = spark.sql(
    f"""SELECT * FROM auofab.prod_dw.dt_all_s_all_dw_at_feedback_h_dax_fbk_master_ods LIMIT 1""")
master.display()

target_schema = StructType(master.schema.fields)
empty_target_df = spark.createDataFrame(data=[], schema=target_schema)

tablename = "mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods"


target_delta_table_location = f"{save_path}/{tablename}"
empty_target_df.write.format("delta").mode('overwrite').save(target_delta_table_location)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {delta_schema}.{tablename}
            USING DELTA LOCATION '{target_delta_table_location}'
""")

# COMMAND ----------

tablename = "mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods"


target_delta_table_location = f"{save_path}/{tablename}"


a = spark.table(f"{delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods")
# column_names = [col for col in a.columns if col not in ['RRN_LAST_ONE_NUM']]
# a = a.selectExpr("'N' AS ARRAY_UPDATE_FLAG", "'N' AS FEOL_UPDATE_FLAG", "'N' AS BEOL_UPDATE_FLAG", *column_names)
a.write.format("delta").mode('overwrite').option("overwriteSchema", "true").save(target_delta_table_location)
# a.display()

# COMMAND ----------

a = spark.table(f"auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods").selectExpr("CAST(CREATE_DTM AS DATE) AS CREATE_DATE", "*")
a.display()
a.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods")

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods CLUSTER BY (CREATE_DATE)")

# COMMAND ----------

spark.sql(f'ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)') 

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods CLUSTER BY (ARRAY_UPDATE_FLAG, FEOL_UPDATE_FLAG, FEOL_FAB_CODE, BEOL_UPDATE_FLAG)")

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods CLUSTER BY (CREATE_DTM)")

# COMMAND ----------

spark.sql(f"ALTER TABLE {delta_schema}.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE auofabdev.prod_dw.mf_all_s_all_kpi_at_feedback_h_dax_fbk_master_ods

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SET JOB CONTRAL TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete JOB

# COMMAND ----------

def get_delete_sql(stage):
  crontab = f"DELETE FROM {jscs_db_schema}.crontab WHERE job_name = '{stage}_H_DAX_FBK_MASTER_TEMP'"
  r_ctl_etl = f"DELETE FROM {jscs_db_schema}.r_ctl_etl WHERE job_name = '{stage}_H_DAX_FBK_MASTER_TEMP'"
  return crontab, r_ctl_etl

# COMMAND ----------

stage = 'ARRAY'
# for fab in ["L3C", "L3D", "L4A", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L7B", "L8A", "L8B"]:
crontab, r_ctl_etl = get_delete_sql(stage)
spark.sql(crontab)
spark.sql(r_ctl_etl)

# COMMAND ----------

stage = 'FEOL'
# for fab in ["L3C", "L3D", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L8A", "L8B"]:
crontab, r_ctl_etl = get_delete_sql(stage)
spark.sql(crontab)
spark.sql(r_ctl_etl)

# COMMAND ----------

stage = 'BEOL'
# for fab in ["L3C", "L3D", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L8A", "L8B"]:
crontab, r_ctl_etl = get_delete_sql(stage)
spark.sql(crontab)
spark.sql(r_ctl_etl)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM auofabdev.prod_dw.crontab WHERE job_name = 'F2C_H_DAX_FBK_MASTER'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ARRAY

# COMMAND ----------

stage = 'ARRAY'
# for fab in ["L3C", "L3D", "L4A", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L7B", "L8A", "L8B"]:
sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{stage}_H_DAX_FBK_MASTER_TEMP', '0 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FEOL

# COMMAND ----------

stage = 'FEOL'
# for fab in ["L3C", "L3D", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L8A", "L8B"]:
sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{stage}_H_DAX_FBK_MASTER_TEMP', '0 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BEOL

# COMMAND ----------

stage = 'BEOL'
# for fab in ["L3C", "L3D", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L8A", "L8B"]:
sql = f"INSERT INTO {jscs_db_schema}.crontab (job_name, schedule, reloads, active_flag, auodw_talbe, where_condition, delta_name, file_name, synapse_talbe, tmp_synapse_talbe, key_value, query_condition, tmp_delta_name) VALUES ('{stage}_H_DAX_FBK_MASTER_TEMP', '0 * * * *', 1, 'Y', '', '', '', '', '', '', '', '', '')"
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC # 平展 JOB

# COMMAND ----------

DEPLOY_FILE_PATH = "../app_config/deploy.json"

workflow_path_dict = {
    "MASTER": "../app_config/master.json",
}

def fab(stage: str):
    fab_dict = {
        'ARRAY': ['L3C', 'L3D', 'L4A', 'L5B', 'L5C', 'L5D', 'L6A', 'L6B', 'L6K', 'L7A', 'L7B', 'L8A', 'L8B'],
        'FEOL': ['L3C', 'L3D', 'L5B', 'L5C', 'L5D', 'L6A', 'L6B', 'L6K', 'L7A', 'L8A', 'L8B'],
        'BEOL': ['L3C', 'L3D', 'L5B', 'L5C', 'L5D', 'L6A', 'L6B', 'L6K', 'L7A', 'L8A', 'L8B'],
    }
    return fab_dict.get(stage)

with open(DEPLOY_FILE_PATH) as f:
    DEPLOY_JSON = json.load(f)

# COMMAND ----------

class JobMigration:
    def __init__(self, deploy_env: str, which_job: str) -> None:
        """
            :deploy_env : "QA" or "PRD"
            :which_job : "L4_L5_L6" or "L6_IMS"
        """
        self.deploy_env = deploy_env
        self.which_job = which_job
        self.deploy_json = DEPLOY_JSON[deploy_env]

        self.domain = self.deploy_json["DOMAIN"]
        self.token = self.deploy_json["TOKEN"]

    @property
    def src_job(self) -> None:
        path = workflow_path_dict[self.which_job]
        with open(path) as file:
            workflow_json = json.load(file)
        print(f"Load `{self.which_job}` src job structure.")
        return workflow_json

    def create_job(self) -> None:
        response = requests.post(
            url=f"https://{self.domain}/api/2.1/jobs/create",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.src_job
        )
        if response.status_code == 200:
            print(
                f"""Create {self.deploy_env} JOB\n JOB_NAME: {self.which_job}.""")
        else:
            print(
                f"""Error\nerror_code: {response.json()["error_code"]}\nerror_message: {response.json()["message"]}""")

    @property
    def revised_job(self) -> None:
        dest_job = self.src_job
        dest_job["job_id"] = self.target_job_id
        dest_job["settings"]["name"] = self.target_job_name
        dest_job["settings"]["git_source"]["git_branch"] = self.target_branch
        dest_job['settings']['tasks'] = []

        
        for fab in self.fab_list:
            fab_set_l2_arg = {
                "task_key": fab + "_set_l2_args",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                "notebook_path": "master/l2/l2_set_job_args",
                "base_parameters": {
                    "START_TIME": "",
                    "END_TIME": "",
                    "FAB": fab,
                    "PROCESS_STAGE": self.stage,
                    "FAB_CODE": self.fab_code_dict[fab]
                },
                "source": "GIT"
                },
                "job_cluster_key": "Job_cluster",
                "max_retries": 3,
                "min_retry_interval_millis": 30000,
                "retry_on_timeout": False,
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
                },
                "webhook_notifications": {}
            }

            master = {
                    "task_key": f"{fab}_{self.stage.lower()}_h_dax_fbk_master_temp",
                    "depends_on": [
                    {
                        "task_key": fab + "_set_l2_args"
                    }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                    "notebook_path": f"master/l2/main_{self.stage.lower()}",
                    "base_parameters": {
                        "JOB_NAME": f"{fab}_{self.stage}_H_DAX_FBK_MASTER_TEMP",
                        "TASK_KEY":fab + "_set_l2_args"
                    },
                    "source": "GIT"
                    },
                    "job_cluster_key": "Job_cluster",
                    "max_retries": 3,
                    "min_retry_interval_millis": 30000,
                    "retry_on_timeout": False,
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                    },
                    "webhook_notifications": {}
                }
            
            # task append
            dest_job['settings']['tasks'].append(fab_set_l2_arg)
            dest_job['settings']['tasks'].append(master)  
        dest_job["new_settings"] = dest_job.pop("settings")
        dest_job
        return dest_job 
            
    def reset_job(self, 
                  stage: str,
                  target_job_id: str,
                  target_job_name: str,
                  target_branch: str,
                  target_task_name: str = "set_l2_args") -> None:
        self.target_job_id = target_job_id
        self.target_job_name = target_job_name
        self.target_branch = target_branch
        self.target_task_name = target_task_name
        self.stage = stage
        self.fab_list = fab(stage)
        self.fab_code_dict = config.fab_code_dict(stage)

        response = requests.post(
            url=f"https://{self.domain}/api/2.1/jobs/reset",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.revised_job
        )
        if response.status_code == 200:
            print(
                f"""Reset {self.deploy_env} JOB\n\tJOB_NAME: {self.which_job}\n\tJOB_ID = {self.target_job_id}.""")
        else:
            print(
                f"""Error\nerror_code: {response.json()["error_code"]}\nerror_message: {response.json()["message"]}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 更新json(Array)

# COMMAND ----------

env = "DEV"
job_id = 426039597082595 if env == "PRD" else 149105404218917
job_name = "FBK_Master_Array_ODSToTemp"
branch = "main"
L2_L3 = JobMigration(deploy_env=env, which_job="MASTER")
L2_L3.reset_job(stage="ARRAY", target_job_id=job_id, target_job_name=job_name, target_branch=branch)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 更新json(Feol)

# COMMAND ----------

env = "DEV"
job_id = 669588176624675 if env == "PRD" else 417565198626329
job_name = "FBK_Master_Feol_ODSToTemp"
branch = "main"
L2_L3 = JobMigration(deploy_env=env, which_job="MASTER")
L2_L3.reset_job(stage="FEOL", target_job_id=job_id, target_job_name=job_name, target_branch=branch)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 更新json(Beol)

# COMMAND ----------


env = "DEV"
job_id = 366454700504763 if env == "PRD" else 1068599530920663
job_name = "FBK_Master_Beol_ODSToTemp"
branch = "main"
L2_L3 = JobMigration(deploy_env=env, which_job="MASTER")
L2_L3.reset_job(stage="BEOL", target_job_id=job_id, target_job_name=job_name, target_branch=branch)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 更新 MASTER TABLE

# COMMAND ----------

class JobMigration:
    def __init__(self, deploy_env: str, which_job: str) -> None:
        """
            :deploy_env : "QA" or "PRD"
            :which_job : "L4_L5_L6" or "L6_IMS"
        """
        self.deploy_env = deploy_env
        self.which_job = which_job
        self.deploy_json = DEPLOY_JSON[deploy_env]

        self.domain = self.deploy_json["DOMAIN"]
        self.token = self.deploy_json["TOKEN"]

    @property
    def src_job(self) -> None:
        path = workflow_path_dict[self.which_job]
        with open(path) as file:
            workflow_json = json.load(file)
        print(f"Load `{self.which_job}` src job structure.")
        return workflow_json

    def create_job(self) -> None:
        response = requests.post(
            url=f"https://{self.domain}/api/2.1/jobs/create",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.src_job
        )
        if response.status_code == 200:
            print(
                f"""Create {self.deploy_env} JOB\n JOB_NAME: {self.which_job}.""")
        else:
            print(
                f"""Error\nerror_code: {response.json()["error_code"]}\nerror_message: {response.json()["message"]}""")

    @property
    def revised_job(self) -> None:
        def fab_2(stage: str):
            fab_dict = {
                'ARRAY': ['L3C', 'L3D', 'L4A', 'L5B', 'L5C', 'L5D', 'L6A', 'L6B', 'L6K', 'L7A', 'L7B', 'L8A', 'L8B'],
                'FEOL': ['L3C', 'L3D', 'L5B', 'L5C', 'L5D', 'L6A', 'L6B', 'L6K', 'L7A', 'L8A', 'L8B'],
                'BEOL': ['L3C', 'L3D', 'L5B', 'L5C', 'L5D', 'L6A', 'L6B', 'L6K', 'L7A', 'L8A', 'L8B']
            }
            return fab_dict.get(stage)
          
        dest_job = self.src_job
        dest_job["job_id"] = self.target_job_id
        dest_job["settings"]["name"] = self.target_job_name
        dest_job["settings"]["git_source"]["git_branch"] = self.target_branch
        dest_job['settings']['tasks'] = []

        for stage in ['ARRAY', 'FEOL', 'BEOL']:
          fab_list = fab_2(stage)
          fab_code_dict = config.fab_code_dict(stage)
          for fab in fab_list:
            fab_set_l2_arg = {
                "task_key": f"{fab}_{stage.lower()}_set_l2_args",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                "notebook_path": "master/l2/l2_set_job_args",
                "base_parameters": {
                    "START_TIME": "",
                    "END_TIME": "",
                    "FAB": fab,
                    "PROCESS_STAGE": stage,
                    "FAB_CODE": fab_code_dict[fab]
                },
                "source": "GIT"
                },
                "job_cluster_key": "Job_cluster",
                "max_retries": 3,
                "min_retry_interval_millis": 30000,
                "retry_on_timeout": False,
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
                },
                "webhook_notifications": {}
            }

            master = {
                    "task_key": f"{fab}_{stage.lower()}_h_dax_fbk_master_temp",
                    "depends_on": [
                    {
                        "task_key": f"{fab}_{stage.lower()}_set_l2_args"
                    }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                    "notebook_path": f"master/l2/main_{stage.lower()}",
                    "base_parameters": {
                        "JOB_NAME": f"{fab}_{stage}_H_DAX_FBK_MASTER_TEMP",
                        "TASK_KEY": f"{fab}_{stage.lower()}_set_l2_args"
                    },
                    "source": "GIT"
                    },
                    "job_cluster_key": "Job_cluster",
                    "max_retries": 3,
                    "min_retry_interval_millis": 30000,
                    "retry_on_timeout": False,
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                    },
                    "webhook_notifications": {}
                }
              
            # task append
            dest_job['settings']['tasks'].append(fab_set_l2_arg)
            dest_job['settings']['tasks'].append(master)  
        dest_job["new_settings"] = dest_job.pop("settings")
        return dest_job 
            
    def reset_job(self, 
                  stage: str,
                  target_job_id: str,
                  target_job_name: str,
                  target_branch: str,
                  target_task_name: str = "set_l2_args") -> None:
        self.target_job_id = target_job_id
        self.target_job_name = target_job_name
        self.target_branch = target_branch
        self.target_task_name = target_task_name
        self.stage = stage
        self.fab_list = fab(stage)
        self.fab_code_dict = config.fab_code_dict(stage)

        response = requests.post(
            url=f"https://{self.domain}/api/2.1/jobs/reset",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.revised_job
        )
        if response.status_code == 200:
            print(
                f"""Reset {self.deploy_env} JOB\n\tJOB_NAME: {self.which_job}\n\tJOB_ID = {self.target_job_id}.""")
        else:
            print(
                f"""Error\nerror_code: {response.json()["error_code"]}\nerror_message: {response.json()["message"]}""")

# COMMAND ----------


env = "DEV"
job_id = 366454700504763 if env == "PRD" else 1015194248884251
job_name = "FBK_Master_ODSToCentral"
branch = "main"
L2_L3 = JobMigration(deploy_env=env, which_job="MASTER")
L2_L3.reset_job(stage="BEOL", target_job_id=job_id, target_job_name=job_name, target_branch=branch)
