# Databricks notebook source
# MAGIC %pip install croniter

# COMMAND ----------

from argparse import Namespace

from util.taskvalue import TaskValue
from util.jscs import JscsJob, get_start_end_time_for_sourcedata_delay
from test.config import Config

# COMMAND ----------

config = Config()

db_schema = config.delta_schema
save_path = config.save_path

try:
  taskKey = dbutils.widgets.get('TASK_KEY')
  JOB_STATUS = dbutils.widgets.get('JOB_STATUS')
except:
  taskKey = config.l2_taskName

taskvalue = TaskValue()
workflow_time = taskvalue.get_args(taskKey=taskKey,
                                   key=config.taskKey,
                                   debugType="l2_l3")
n = Namespace(**workflow_time)

# COMMAND ----------

# JSCS 傳參
jscs_time = {
    "LAST_DSP": "",
    "LAST_DEP": "",
}

# 取得 JOB 資訊
JOB_NAME = n.JOB_NAME
START_TIME = n.START_TIME
END_TIME = n.END_TIME

jscs = JscsJob(JOB_NAME, config.jscs_db_schema, is_notimezone=False)
jscs.get_job_dsp_dep()

# 判斷是否重新更新
if START_TIME == "" and  END_TIME == "":
    JOB_RUN = jscs.get_job_run()
    START_TIME, END_TIME = get_start_end_time_for_sourcedata_delay(jscs, get_time=True)
    jscs_time['LAST_DSP'], jscs_time['LAST_DEP'] = jscs.get_dsp_dep_str()
    print(f"Start: {START_TIME}, End: {END_TIME}")
else:
    JOB_RUN = jscs.get_job_run(True)
    print(f"Start: {START_TIME}, End: {END_TIME}")

taskvalue = TaskValue()
taskvalue.set_args(key=config.jscs_taskKey, value=jscs_time)

# COMMAND ----------

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
