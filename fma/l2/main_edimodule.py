# Databricks notebook source
# MAGIC %md
# MAGIC # EDI MODULE FMA

# COMMAND ----------

#20250210 查詢地上雲FMA ODS無資料。不做開發。 程式copy不做任何處理 


# COMMAND ----------

# MAGIC %pip install croniter

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------


import ast
from argparse import Namespace

from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from util.taskvalue import TaskValue
from lib.TFT_CHIP_ID_Trans import generate_chip_id_list
from fma.config import Config   # Config 放在各自的資料夾
from fma.l2.config.kpi_info import TableInfo, KpiInfo
from fma.l2.src.running.edimodule import EdiModuleFma
from fma.l2.logic.stage_args import EdiModuleLogicArgs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Job args

# COMMAND ----------

config = Config()

db_schema = config.delta_schema
save_path = config.save_path

try:
  taskKey = dbutils.widgets.get('TASK_KEY')
except:
  taskKey = config.l2_taskName

taskvalue = TaskValue()
workflow_time = taskvalue.get_args(taskKey=taskKey,
                                   key=config.taskKey,
                                   debugType="l2_l3")
n = Namespace(**workflow_time)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Global Setup

# COMMAND ----------

IS_TEST = True

# Table Name
FMA_TABLE = config.h_dax_fbk_fma_ods
FMA_TABLENAME = config.mf_all_s_all_dw_at_feedback_h_dax_fbk_fma_ods
print(f"FMA_TABLE:{FMA_TABLE} , FMA_TABLENAME:{FMA_TABLENAME}")


if IS_TEST:
    START_TIME = "2025-01-20 01:00:00"
    END_TIME = "2025-01-20 23:59:00"
    PROCESS_STAGE = "EDIMODULE"
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
    FAB = "F20"
    FAB_CODE = FAB_CODE_DICT.get(FAB)
    JOB_RUN = True
    IS_RUN = "TRUE"
else:
    START_TIME = n.START_TIME
    END_TIME = n.END_TIME
    JOB_NAME = n.JOB_NAME
    FAB = n.FAB
    PROCESS_STAGE = n.PROCESS_STAGE    
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
    FAB_CODE = config.fab_code_dict(PROCESS_STAGE).get(FAB)
    IS_RUN = n.IS_RUN

    if not JOB_NAME.strip():  # 如果去掉空格后字符串为空
        JOB_NAME = f"{FAB}_{PROCESS_STAGE}_F2C_H_DAX_FBK_FMA"  

    # JSCS
    jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
    jscs.get_job_dsp_dep()

    if START_TIME == "" and  END_TIME == "":
        # jscs 取得撈取時間
        JOB_RUN = jscs.get_job_run()
        START_TIME, END_TIME = get_start_end_time_for_sourcedata_delay(jscs, get_time=True)
        print(f"Start: {START_TIME}, End: {END_TIME}")
    else:
        JOB_RUN = jscs.get_job_run(True)
        print(f"Start: {START_TIME}, End: {END_TIME}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # EDI MODUE FMA TABLE

# COMMAND ----------

if (JOB_RUN and IS_RUN == "TRUE"):

    try:
        if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = EdiModuleLogicArgs(
            START_TIME,
            END_TIME,
            FAB_CODE_DICT,
            FAB,
            FAB_CODE,
            PROCESS_STAGE
        )

        Steps = {
            "fma": KpiInfo("None", ['K01','K06','L6K','M02','M11','S01','S02','S06','S11','S17'], logic_args)
        }

        Info = TableInfo(
            fab = FAB,
            process_stage=PROCESS_STAGE,
            target_table=FMA_TABLE,
            target_table_name=FMA_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="upsert",
            delete_clause=None,
            kpi_steps=Steps
        )

        fma= EdiModuleFma(
            table_info=Info,
        )
        fma.prepare_data()
        # fma.delete_upsert_data()

        if not IS_TEST: jscs.update_job_status("COMP")
    
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this EDI Module run because the JOB_RUN is {JOB_RUN}.")
