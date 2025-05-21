# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # FEOL Master

# COMMAND ----------

import ast
from argparse import Namespace

from util.jscs import JscsJob, get_start_end_time_for_master
from util.taskvalue import TaskValue
from master.config import Config   # Config 放在各自的資料夾
from master.l2.config.kpi_info import TableInfo, KpiInfo
from master.l2.src.running.feol import FeolMaster
from master.l2.logic.stage_args import FeolLogicArgs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Job args

# COMMAND ----------

config = Config()

db_schema = config.delta_schema
save_path = config.save_path

try:
  PROCESS_STAGE = dbutils.widgets.get('PROCESS_STAGE')
except:
  taskKey = config.l2_taskName
taskvalue = TaskValue()
workflow_time = taskvalue.get_args(taskKey=config.l2_taskName,
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
MASTER_TABLENAME = config.mf_all_b_cel_kpi_at_feedback_h_dax_fbk_master_ods_feol_temp

if IS_TEST:
    START_TIME = "2024-08-08 00:00:00"
    END_TIME = "2024-08-08 12:00:00"
    PROCESS_STAGE = "FEOL"
    RUN_FAB_LIST = ["L3C", "L3D", "L5B", "L5C", "L5D", "L6A", "L6B", "L6K", "L7A", "L8A", "L8B"]
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)
     # JOB_NAME = f"F2C_H_DAX_FBK_MASTER"
    JOB_RUN = True
else:
    START_TIME = n.START_TIME
    END_TIME = n.END_TIME
    JOB_NAME = n.JOB_NAME
    RUN_FAB_LIST = n.FEOL_RUN_FAB
    FAB_CODE_DICT = config.fab_code_dict(PROCESS_STAGE)

    # JSCS
    jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
    jscs.get_job_dsp_dep()

    if START_TIME == "" and  END_TIME == "":
        # jscs 取得撈取時間
        JOB_RUN = jscs.get_job_run()
        START_TIME, END_TIME = get_start_end_time_for_master(jscs, get_time=True)
        print(f"Start: {START_TIME}, End: {END_TIME}")
    else:
        JOB_RUN = jscs.get_job_run(True)
        print(f"Start: {START_TIME}, End: {END_TIME}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FEOL MASTER TABLE

# COMMAND ----------

if (JOB_RUN):

    try:
        # if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = FeolLogicArgs(
            START_TIME,
            END_TIME,
            RUN_FAB_LIST,
            FAB_CODE_DICT,
            config.mapping_fab_list(),
            PROCESS_STAGE
        )

        Steps = {
            "master": KpiInfo("None", None, logic_args)
        }

        Info = TableInfo(
            process_stage=PROCESS_STAGE,
            target_table=MASTER_TABLENAME,
            target_table_name=MASTER_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="overwrite",
            delete_clause=None,
            kpi_steps=Steps
        )


        master = FeolMaster(
            table_info=Info,
        )
        master.prepare_data()
        master.delete_upsert_data()

        # if not IS_TEST: jscs.update_job_status("COMP")
        
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this cell run because the JOB_RUN is {JOB_RUN}.")
