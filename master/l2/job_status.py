# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

from argparse import Namespace

from util.taskvalue import TaskValue
from util.jscs import JscsJob, get_start_end_time_for_master
from master.config import Config 

# COMMAND ----------

JOB_STATUS = dbutils.widgets.get('JOB_STATUS')

config = Config()
taskvalue = TaskValue()
workflow_time = taskvalue.get_args(taskKey=config.l2_taskName,
                                   key=config.taskKey,
                                   debugType="l2_l3")
job_args = Namespace(**workflow_time)

# COMMAND ----------

# 取得 JOB 資訊
JOB_NAME = job_args.JOB_NAME
START_TIME = job_args.START_TIME
END_TIME = job_args.END_TIME

jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
jscs.get_job_dsp_dep()

# 判斷是否重新更新
if START_TIME == "" and  END_TIME == "":
    # jscs 取得撈取時間
    JOB_RUN = jscs.get_job_run()
    START_TIME, END_TIME = get_start_end_time_for_master(jscs, get_time=True)
    print(f"Start: {START_TIME}, End: {END_TIME}")
else:
    JOB_RUN = jscs.get_job_run(True)
    print(f"Start: {START_TIME}, End: {END_TIME}")

# COMMAND ----------

# DBTITLE 1,運行狀態
if JOB_STATUS == 'FAIL': 
  # 運行失敗
  jscs.update_job_status("FAIL", err)
elif JOB_STATUS == 'COMP':
  # 運行成功
  jscs.update_job_status("COMP")
elif JOB_STATUS == 'RUN':
  # 運行成功
  jscs.update_job_status("RUN")
else:
  raise ValueError(f"Invalid JOB_STATUS: {JOB_STATUS}")
