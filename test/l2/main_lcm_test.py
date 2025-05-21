# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # LCM Test

# COMMAND ----------

from argparse import Namespace
from pyspark.sql import functions as F

from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from util.taskvalue import TaskValue
from test.config import Config   # Config 放在各自的資料夾
from test.l2.config.kpi_info import TableInfo, KpiInfo
from test.l2.src.running.lcm_test import LcmTestTest
from test.l2.logic.stage_args import LcmTestLogicArgs

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
jscs_time = taskvalue.get_args(taskKey="job_status_run",
                                   key=config.jscs_taskKey,
                                   debugType="l2_l3")                                   
n = Namespace(**workflow_time)
jscs_time = Namespace(**jscs_time)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Global Setup

# COMMAND ----------

IS_TEST = False

# Table Name
TEST_TABLE = config.h_dax_fbk_test_ods
TEST_TABLENAME = config.mf_all_s_all_dw_at_feedback_h_dax_fbk_test_ods
print(f"TEST_TABLE:{TEST_TABLE} , TEST_TABLENAME:{TEST_TABLENAME}")


if IS_TEST:
    START_TIME = "2025-01-24 06:30:00"
    END_TIME = "2025-02-04 08:30:00"
    JOB_RUN = True
else:
    START_TIME = n.START_TIME
    END_TIME = n.END_TIME
    JOB_NAME = n.JOB_NAME 

    # JSCS
    jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
    jscs.set_job_dsp_dep(jscs_time.LAST_DSP, jscs_time.LAST_DEP)

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
# MAGIC # LCM TEST TABLE

# COMMAND ----------

if (JOB_RUN):

    try:
        # if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = LcmTestLogicArgs(
            START_TIME,
            END_TIME,
            fab = "ALL",
            process_stage="LCM_TEST",
        )

        Steps = {
            "test": KpiInfo("None", ['ALL'], logic_args)
        }

        Info = TableInfo(
            fab = "ALL",
            process_stage="LCM_TEST",
            target_table=TEST_TABLE,
            target_table_name=TEST_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="upsert",
            delete_clause=None,
            kpi_steps=Steps
        )

        test = LcmTestTest(
            table_info=Info,
        )
        test.prepare_data()
        test.delete_upsert_data()

        if not IS_TEST: jscs.update_job_status("COMP")
    
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this cell run because the JOB_RUN is {JOB_RUN}.")
