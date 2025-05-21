# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

# MAGIC %md
# MAGIC # MODULE Scrap

# COMMAND ----------

from argparse import Namespace

from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from util.taskvalue import TaskValue
from scrap.config import Config
from scrap.l2.config.kpi_info import TableInfo, KpiInfo
from scrap.l2.src.running.module import Scrap
from scrap.l2.logic.stage_args import ModuleLogicArgs

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
SCRAP_TABLENAME = config.mf_all_s_all_dw_at_feedback_h_dax_fbk_scrap_ods
print(f"SCRAP_TABLENAME:{SCRAP_TABLENAME}")


if IS_TEST:
    START_TIME = "2024-12-27 00:00:00"
    END_TIME = "2024-12-27 09:00:00"
    PROCESS_STAGE = "MODULE"
    FAB_CODE_DICT = config.fab_code_dict()
    FAB = "S01"
    FAB_CODE = FAB_CODE_DICT.get(FAB)
    JOB_RUN = True
    IS_RUN = "TRUE"
else:
    START_TIME = n.START_TIME
    END_TIME = n.END_TIME
    JOB_NAME = n.JOB_NAME
    FAB = n.FAB
    PROCESS_STAGE = n.PROCESS_STAGE    
    FAB_CODE_DICT = config.fab_code_dict()
    FAB_CODE = config.fab_code_dict().get(FAB)
    IS_RUN = n.IS_RUN

    if not JOB_NAME.strip():  # 如果去掉空格后字符串为空
        JOB_NAME = f"{FAB}_{PROCESS_STAGE}_F2C_H_DAX_FBK_SCRAP"  

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
# MAGIC # MODULE SCRAP TABLE

# COMMAND ----------

if (JOB_RUN and IS_RUN == "TRUE"):

    try:
        if not IS_TEST: jscs.update_job_status("RUN")

        logic_args = ModuleLogicArgs(
            START_TIME,
            END_TIME,
            FAB,
            FAB_CODE,
            PROCESS_STAGE
        )

        steps = {
            "scrap": KpiInfo("None", ['K01', 'K06', 'L6K', 'L8B', 'M02', 'M11', 'S01', 'S02', 'S06', 'S11'], logic_args)
        }

        info = TableInfo(
            fab = FAB,
            process_stage=PROCESS_STAGE,
            target_table=SCRAP_TABLENAME,
            target_table_name=SCRAP_TABLENAME,
            db_schema=db_schema,
            save_path=save_path,
            update_method="upsert",
            delete_clause=None,
            kpi_steps=steps
        )

        table = Scrap(
            table_info=info,
        )
        table.prepare_data()
        table.delete_upsert_data()

        if not IS_TEST: jscs.update_job_status("COMP")
    
    except Exception as err:
        print("Error: ", err)
        if not IS_TEST: jscs.update_job_status("FAIL", err)
        raise
else:
    print(f"Skip this cell run because the JOB_RUN is {JOB_RUN}.")
