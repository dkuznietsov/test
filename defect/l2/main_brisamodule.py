# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.rangeJoin.enabled", "true")  

# COMMAND ----------

# MAGIC %md
# MAGIC # OEM Module Defect

# COMMAND ----------

from argparse import Namespace

from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from util.taskvalue import TaskValue
from defect.config import Config
from defect.l2.config.kpi_info import TableInfo, KpiInfo
from defect.l2.src.running.brisamodule import BrisaModuleDefect
from defect.l2.logic.stage_args import BrisaModuleLogicArgs

# COMMAND ----------

# MAGIC %md
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
# MAGIC # Global Setup

# COMMAND ----------

IS_TEST = False
#job False

# Table Name
DEFECT_TABLE = config.h_dax_fbk_defect_ods
DEFECT_TABLENAME = config.mf_all_s_all_dw_at_feedback_h_dax_fbk_defect_ods
print(f"DEFECT_TABLE:{DEFECT_TABLE} , DEFECT_TABLENAME:{DEFECT_TABLENAME}")


if IS_TEST:
    START_TIME = "2025-02-24 14:00:00"
    END_TIME = "2025-02-26 21:30:00"
    PROCESS_STAGE = "BRISAMODULE"
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
    FAB = "Z40"
    FAB_CODE = FAB_CODE_DICT.get(FAB)
    TEST_TYPE = config.test_type_dict().get(FAB)
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
    TEST_TYPE = config.test_type_dict().get(FAB)
    IS_RUN = n.IS_RUN

    # JSCS
    jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
    jscs.get_job_dsp_dep()

    if START_TIME == "" and  END_TIME == "":
        # jscs 取得撈取時間
        JOB_RUN = jscs.get_job_run()
        START_TIME, END_TIME = get_start_end_time_for_sourcedata_delay(jscs, reloaddep=True, get_time=True)
        print(f"Start: {START_TIME}, End: {END_TIME}")
    else:
        JOB_RUN = jscs.get_job_run(True)
        print(f"Start: {START_TIME}, End: {END_TIME}")

# COMMAND ----------

# MAGIC %md
# MAGIC # MODULE DEFECT TABLE

# COMMAND ----------

if (JOB_RUN and IS_RUN == "TRUE"):

    try:
        if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = BrisaModuleLogicArgs(
            START_TIME,
            END_TIME,
            FAB_CODE_DICT,
            FAB,
            FAB_CODE,
            PROCESS_STAGE,
            TEST_TYPE
        )

        Steps = {
            "defect": KpiInfo("None", ['S13','X13','Z0D','Z0M','Z1E','Z1F','Z1L','Z22','Z31','Z45','Z94','ZF2','ZK7','ZKC','Z40'], logic_args)
        }

        Info = TableInfo(
            fab = FAB,
            process_stage=PROCESS_STAGE,
            target_table=DEFECT_TABLE,
            target_table_name=DEFECT_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="upsert",
            delete_clause=None,
            kpi_steps=Steps
        )

        defect = BrisaModuleDefect(
            table_info=Info,
        )
        defect.prepare_data()
        defect.delete_upsert_data()

        if not IS_TEST: jscs.update_job_status("COMP")
    
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this Module run because the JOB_RUN is {JOB_RUN}.")
