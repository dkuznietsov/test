# Databricks notebook source
# spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # MODULE Test

# COMMAND ----------

import ast
from argparse import Namespace

from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from util.taskvalue import TaskValue
from test.config import Config   # Config 放在各自的資料夾
from test.l2.config.kpi_info import TableInfo, KpiInfo
from test.l2.src.running.module import ModuleTest
from test.l2.logic.stage_args import ModuleLogicArgs

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
  FAB = dbutils.widgets.get('FAB')
  PROCESS_STAGE = dbutils.widgets.get('PROCESS_STAGE')
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
TEST_TABLENAME = config.mf_all_s_mdl_dw_at_feedback_h_dax_fbk_test_ods_temp
print(f"TEST_TABLE:{TEST_TABLE} , TEST_TABLENAME:{TEST_TABLENAME}")


if IS_TEST:
    START_TIME = "2025-01-24 06:30:00"
    END_TIME = "2025-02-04 08:30:00"
    PROCESS_STAGE = "MODULE"
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
    FAB = "L6K"
    FAB_CODE = FAB_CODE_DICT.get(FAB)
    TEST_OP = config.test_op_dict().get(FAB)
    JOB_RUN = True
    IS_RUN = "TRUE"
else:
    START_TIME = n.START_TIME
    END_TIME = n.END_TIME
    JOB_NAME = n.JOB_NAME
    RUN_FAB_LIST = n.MODULE_RUN_FAB
  
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
    FAB_CODE = config.fab_code_dict(PROCESS_STAGE).get(FAB)
    TEST_OP = config.test_op_dict().get(FAB)

    IS_RUN = "TRUE" if FAB in RUN_FAB_LIST else "FALSE"  
    print(f"IS_RUN = {IS_RUN}")

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
# MAGIC # MODULE TEST TABLE

# COMMAND ----------

if (JOB_RUN and IS_RUN == "TRUE"):

    try:
        # if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = ModuleLogicArgs(
            START_TIME,
            END_TIME,
            FAB_CODE_DICT,
            FAB,
            FAB_CODE,
            PROCESS_STAGE,
            TEST_OP
        )

        Steps = {
            "test": KpiInfo("None", ['M02', 'M11', 'S01', 'K01', 'S02', 'S03', 'S11', 'S06', 'K06', 'L6K', 'S17'], logic_args),
            "test_ods": KpiInfo("None", ['L6K', 'L6B', 'L8B'], logic_args)
        }

        Info = TableInfo(
            fab = FAB,
            process_stage=PROCESS_STAGE,
            target_table=TEST_TABLE,
            target_table_name=TEST_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="insert",
            delete_clause=f"DELETE FROM {db_schema}.{TEST_TABLENAME} WHERE FAB_CODE = '{FAB_CODE}' AND PROCESS_STAGE = 'MODULE'",
            kpi_steps=Steps
        )

        test = ModuleTest(
            table_info=Info,
        )
        test.prepare_data()
        test.delete_upsert_data()

        # if not IS_TEST: jscs.update_job_status("COMP")
    
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this cell run because the JOB_RUN is {JOB_RUN}.")
