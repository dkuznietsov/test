# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # ARRAY pcccode

# COMMAND ----------

import ast
from argparse import Namespace

from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from util.taskvalue import TaskValue
from pcccode.config import Config   # Config 放在各自的資料夾
from pcccode.l2.config.kpi_info import TableInfo, KpiInfo
from pcccode.l2.src.running.array import ArrayPcccode
from pcccode.l2.logic.stage_args import ArrayLogicArgs

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

IS_TEST = False

# Table Name
PCCCODE_TABLE = config.h_dax_fbk_pcccode_ods
PCCCODE_TABLENAME = config.mf_all_s_all_dw_at_feedback_h_dax_fbk_pcccode_ods
print(f"PCCCODE_TABLE:{PCCCODE_TABLE} , PCCCODE_TABLENAME:{PCCCODE_TABLENAME}")


if IS_TEST:
    START_TIME = "2024-11-29 16:00:00"
    END_TIME = "2024-12-01 19:00:00"
    PROCESS_STAGE = "ARRAY"
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
    FAB = "L6B"
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
        JOB_NAME = f"{FAB}_{PROCESS_STAGE}_F2C_H_DAX_FBK_PCCCODE"  

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
# MAGIC # ARRAY pcccode TABLE

# COMMAND ----------

if (JOB_RUN and IS_RUN == "TRUE"):

    try:
        if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = ArrayLogicArgs(
            START_TIME,
            END_TIME,
            FAB_CODE_DICT,
            FAB,
            FAB_CODE,
            PROCESS_STAGE
        )

        Steps = {
            "pcccode": KpiInfo("None", ['L6B'], logic_args)
        }

        Info = TableInfo(
            fab = FAB,
            process_stage=PROCESS_STAGE,
            target_table=PCCCODE_TABLE,
            target_table_name=PCCCODE_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="upsert",
            delete_clause=None,
            kpi_steps=Steps
        )

        pcccode= ArrayPcccode(
            table_info=Info,
        )
        pcccode.prepare_data()
        pcccode.delete_upsert_data()

        if not IS_TEST: jscs.update_job_status("COMP")
    
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this cell run because the JOB_RUN is {JOB_RUN}.")
