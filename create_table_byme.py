# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods;

# COMMAND ----------

# Remove the directory if it exists
dbutils.fs.rm(
    "abfss://ittraintable@itadls.dfs.core.windows.net/hive/warehouse/prod_dw.db/mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods",True 
)

# COMMAND ----------

# MAGIC %md
# MAGIC pcccode

# COMMAND ----------

# %sql 這是按照defect做的 之後要建立 新的table
# CREATE TABLE IF NOT EXISTS auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_pcccode_ods
# (
#   FAB_CODE                DOUBLE,
#   PROCESS_STAGE           STRING,
#   CREATE_DTM              TIMESTAMP,
#   SITE_TYPE               STRING,
#   SITE_ID                 STRING,
#   TOOL_ID                 STRING,
#   OP_ID                   STRING,
#   SHIFT                   STRING,
#   ARRAY_LOT_ID            STRING,
#   TFT_GLASS_ID            STRING,
#   TFT_SUB_SHEET_ID        STRING,
#   TFT_CHIP_ID             STRING,
#   PRODUCT_CODE            STRING,
#   ABBR_NO                 STRING,
#   MODEL_NO                STRING,
#   PART_NO                 STRING,
#   TEST_TIME               TIMESTAMP,
#   TEST_USER               STRING,
#   TEST_TYPE               STRING,
#   MFG_DAY                 TIMESTAMP,
#   JUDGE_FLAG              STRING,
#   JUDGE_TYPE              STRING,
#   JUDGE_CRIT              STRING,
#   PRE_GRADE               STRING,
#   GRADE                   STRING,
#   GRADE_CHANGE_FLAG       STRING,
#   JUDGE_CNT               DECIMAL(3,0),
#   TEST_JUDGE_CNT          DECIMAL(3,0),
#   DEFECT_TYPE             STRING,
#   DEFECT_CODE             STRING,
#   DEFECT_CODE_DESC        STRING,
#   DEFECT_VALUE            FLOAT,
#   NOTE_FLAG               STRING,
#   NOTE                    STRING,
#   UPDATE_DTM              TIMESTAMP,
#   CHIP_TYPE               STRING,
#   TFT_CHIP_ID_RRN         DECIMAL(30,0),
#   PRE_MFG_DAY             TIMESTAMP,
#   PRE_TEST_TIME           TIMESTAMP,
#   PRE_TEST_USER           STRING,
#   PRE_SHIFT               STRING,
#   PRE_DEFECT_CODE         STRING,
#   PRE_DEFECT_CODE_DESC    STRING,
#   PRE_DEFECT_VALUE        FLOAT,
#   AUO_CHIP_ID             STRING,
#   INPUT_PART_NO           STRING,
#   DISASSEMBLY_TIME        TIMESTAMP,
#   DEFECT_LOCATION         STRING,
#   QA_USER                 STRING,
#   SHIPPING_NO             STRING,
#   CARTON_NO               STRING,
#   AZ_LM_TIME           TIMESTAMP
# )
# USING delta
# TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
# LOCATION 'abfss://ittraintable@itadls.dfs.core.windows.net/hive/warehouse/prod_dw.db/mf_all_s_all_dw_at_feedback_h_dax_fbk_pcccode_ods';

# COMMAND ----------

# MAGIC %md
# MAGIC material

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_material_ods
# (
#   FAB_CODE                DOUBLE,
#   PROCESS_STAGE           STRING,
#   CREATE_DTM              TIMESTAMP,
#   SITE_TYPE               STRING,
#   SITE_ID                 STRING,
#   TFT_CHIP_ID             STRING,
#   MATERIAL_TYPE           STRING,
#   MATERIAL_VENDOR         STRING,
#   MATERIAL_PART_NO        STRING,
#   MATERIAL_LOT_ID         STRING,
#   UPDATE_DTM              TIMESTAMP,
#   CHIP_TYPE               STRING,
#   MATERIAL_VERSION        STRING,
#   MATERIAL_SLOT_ID        DECIMAL(3,0),
#   ARRAY_LOT_ID            STRING,
#   TFT_GLASS_ID            STRING,
#   TFT_SUB_SHEET_ID        STRING,
#   TFT_CHIP_ID_RRN         DECIMAL(30,0),
#   AUO_CHIP_ID             STRING,
#   INPUT_PART_NO           STRING,
#   DISASSEMBLY_TIME        TIMESTAMP,
#   ASSEMBLY_TIME           TIMESTAMP,
#   MATERIAL_BARCODE        STRING,
#   AZ_LM_TIME           TIMESTAMP
# )
# USING delta
# TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
# LOCATION 'abfss://ittraintable@itadls.dfs.core.windows.net/hive/warehouse/prod_dw.db/mf_all_s_all_dw_at_feedback_h_dax_fbk_material_ods';

# COMMAND ----------

# MAGIC %md
# MAGIC Fma

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods
# (
#   FAB_CODE                DOUBLE,
#   PROCESS_STAGE           STRING,
#   CREATE_DTM              TIMESTAMP,
#   SITE_TYPE               STRING,
#   SITE_ID                 STRING,
#   TOOL_ID                 STRING,
#   OP_ID                   STRING,
#   SHIFT                   STRING,
#   ARRAY_LOT_ID            STRING,
#   TFT_GLASS_ID            STRING,
#   TFT_SUB_SHEET_ID        STRING,
#   TFT_CHIP_ID             STRING,
#   CF_GLASS_ID             STRING,
#   CF_SUB_SHEET_ID         STRING,
#   CF_CHIP_ID              STRING,
#   PRODUCT_CODE            STRING,
#   ABBR_NO                 STRING,
#   MODEL_NO                STRING,
#   PART_NO                 STRING,
#   TEST_TIME               TIMESTAMP,
#   FMA_TIME                TIMESTAMP,
#   FMA_USER                STRING,
#   TEST_TYPE               STRING,
#   MFG_DAY                 TIMESTAMP,
#   JUDGE_FLAG              STRING,
#   TEST_GRADE              STRING,
#   FMA_GRADE               STRING,
#   GRADE_CHANGE_FLAG       STRING,
#   JUDGE_CNT               DOUBLE,
#   TEST_JUDGE_CNT          DOUBLE,
#   DEFECT_TYPE             STRING,
#   DEFECT_CODE             STRING,
#   DEFECT_CODE_DESC        STRING,
#   DEFECT_VALUE            DOUBLE,
#   DEFECT_AREA             STRING,
#   FMA_DEFECT_CODE         STRING,
#   FMA_DEFECT_CODE_DESC    STRING,
#   F_CLASS                 STRING,
#   SIGNAL_NO               DOUBLE,
#   GATE_NO                 DOUBLE,
#   POX_X                   DOUBLE,
#   POX_Y                   DOUBLE,
#   POSITION                STRING,
#   LOCATION_FLAG           STRING,
#   NOTE_FLAG               STRING,
#   NOTE                    STRING,
#   UPDATE_DTM              TIMESTAMP,
#   CHIP_TYPE               STRING,
#   DEFECT_LOCATION         STRING,
#   FMA_CUT_FLAG            STRING,
#   TFT_CHIP_ID_RRN         DECIMAL(30,0),
#   AUO_CHIP_ID             STRING,
#   PNL_REPR_CNT            DOUBLE,
#   INPUT_PART_NO           STRING,
#   DISASSEMBLY_TIME        TIMESTAMP,
#   RESPONSIBILITY          STRING,
#   JUDGE_RESULT            STRING,
#   IMG_FILE_NAME           STRING,
#   IMG_FILE_PATH           STRING,
#   AZ_LM_TIME              TIMESTAMP
# )
# USING delta
# TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
# LOCATION 'abfss://ittraintable@itadls.dfs.core.windows.net/hive/warehouse/prod_dw.db/mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods';

# COMMAND ----------

# MAGIC %md
# MAGIC qa

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods
# MAGIC (
# MAGIC   FAB_CODE                DECIMAL(6,0),
# MAGIC   PROCESS_STAGE           STRING,
# MAGIC   CREATE_DTM              TIMESTAMP,
# MAGIC   SITE_TYPE               STRING,
# MAGIC   SITE_ID                 STRING,
# MAGIC   TOOL_ID                 STRING,
# MAGIC   OP_ID                   STRING,
# MAGIC   SHIFT                   STRING,
# MAGIC   ARRAY_LOT_ID            STRING,
# MAGIC   TFT_GLASS_ID            STRING,
# MAGIC   TFT_SUB_SHEET_ID        STRING,
# MAGIC   TFT_CHIP_ID             STRING,
# MAGIC   PRODUCT_CODE            STRING,
# MAGIC   ABBR_NO                 STRING,
# MAGIC   MODEL_NO                STRING,
# MAGIC   PART_NO                 STRING,
# MAGIC   TEST_TIME               TIMESTAMP,
# MAGIC   TEST_USER               STRING,
# MAGIC   TEST_TYPE               STRING,
# MAGIC   MFG_DAY                 TIMESTAMP,
# MAGIC   JUDGE_FLAG              STRING,
# MAGIC   JUDGE_TYPE              STRING,
# MAGIC   JUDGE_CRIT              STRING,
# MAGIC   PRE_GRADE               STRING,
# MAGIC   GRADE                   STRING,
# MAGIC   GRADE_CHANGE_FLAG       STRING,
# MAGIC   JUDGE_CNT               DECIMAL(3,0),
# MAGIC   TEST_JUDGE_CNT          DECIMAL(3,0),
# MAGIC   DEFECT_TYPE             STRING,
# MAGIC   DEFECT_CODE             STRING,
# MAGIC   DEFECT_CODE_DESC        STRING,
# MAGIC   DEFECT_VALUE            FLOAT,
# MAGIC   NOTE_FLAG               STRING,
# MAGIC   NOTE                    STRING,
# MAGIC   UPDATE_DTM              TIMESTAMP,
# MAGIC   CHIP_TYPE               STRING,
# MAGIC   TFT_CHIP_ID_RRN         DECIMAL(30,0),
# MAGIC   PRE_MFG_DAY             TIMESTAMP,
# MAGIC   PRE_TEST_TIME           TIMESTAMP,
# MAGIC   PRE_TEST_USER           STRING,
# MAGIC   PRE_SHIFT               STRING,
# MAGIC   PRE_DEFECT_CODE         STRING,
# MAGIC   PRE_DEFECT_CODE_DESC    STRING,
# MAGIC   PRE_DEFECT_VALUE        FLOAT,
# MAGIC   AUO_CHIP_ID             STRING,
# MAGIC   INPUT_PART_NO           STRING,
# MAGIC   DISASSEMBLY_TIME        TIMESTAMP,
# MAGIC   DEFECT_LOCATION         STRING,
# MAGIC   QA_USER                 STRING,
# MAGIC   SHIPPING_NO             STRING,
# MAGIC   CARTON_NO               STRING,
# MAGIC   AZ_LM_TIME              TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
# MAGIC LOCATION 'abfss://ittraintable@itadls.dfs.core.windows.net/hive/warehouse/prod_dw.db/mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods CLUSTER BY (FAB_CODE, PROCESS_STAGE);
# MAGIC
# MAGIC optimize auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods;
# MAGIC
# MAGIC ALTER TABLE auofabdev.prod_dw.mf_all_s_all_dw_at_feedback_h_dax_fbk_qa_ods SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
