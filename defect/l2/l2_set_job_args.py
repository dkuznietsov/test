# Databricks notebook source
import ast
import json
from datetime import datetime, time, timedelta
from util.taskvalue import TaskValue
from defect.config import Config

# COMMAND ----------

C2F_START_TIME = dbutils.widgets.get("START_TIME")
C2F_END_TIME = dbutils.widgets.get("END_TIME")
PROCESS_STAGE = dbutils.widgets.get("PROCESS_STAGE")
JOB_NAME = dbutils.widgets.get("JOB_NAME")
FAB = dbutils.widgets.get("FAB")
RUN_FAB_LIST = dbutils.widgets.get(f"{PROCESS_STAGE.upper()}_RUN_FAB")


print(f"F2C_START_TIME = {C2F_START_TIME}")
print(f"F2C_END_TIME = {C2F_END_TIME}")
print(f"JOB_NAME = {JOB_NAME}")
print(f"RUN_FAB_LIST = {RUN_FAB_LIST}")
print(f"FAB = {FAB}")
print(f"PROCESS_STAGE = {PROCESS_STAGE}")

run_fab_list = ast.literal_eval(RUN_FAB_LIST)
IS_RUN = "TRUE" if FAB in run_fab_list else "FALSE"  
print(f"IS_RUN = {IS_RUN}")

config = Config()

workflow_time = {
    "START_TIME": C2F_START_TIME,
    "END_TIME": C2F_END_TIME,
    "JOB_NAME": JOB_NAME,
    "FAB": FAB,
    "PROCESS_STAGE": PROCESS_STAGE,
    "IS_RUN": IS_RUN
}
taskvalue = TaskValue()
taskvalue.set_args(key=config.taskKey, value=workflow_time)

