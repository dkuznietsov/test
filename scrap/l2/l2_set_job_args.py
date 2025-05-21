# Databricks notebook source
import ast
import json
from datetime import datetime, time, timedelta

from util.taskvalue import TaskValue
from test.config import Config

# COMMAND ----------

START_TIME = dbutils.widgets.get("START_TIME")
END_TIME = dbutils.widgets.get("END_TIME")
RUN_FAB_LIST = {
    'MODULE': dbutils.widgets.get("MODULE_RUN_FAB"),
    'BRISA': dbutils.widgets.get("BRISA_RUN_FAB"),
    'EDI': dbutils.widgets.get("EDI_RUN_FAB"),
    'LCM_BEOL': dbutils.widgets.get("LCM_BEOL_RUN_FAB")
}
JOB_NAME = dbutils.widgets.get("JOB_NAME")
FAB = dbutils.widgets.get("FAB")
PROCESS_STAGE = dbutils.widgets.get("PROCESS_STAGE")


print(f"START_TIME = {START_TIME}")
print(f"END_TIME = {END_TIME}")
print(f"JOB_NAME = {JOB_NAME}")
print(f"RUN_FAB_LIST = {RUN_FAB_LIST}")
print(f"FAB = {FAB}")
print(f"PROCESS_STAGE = {PROCESS_STAGE}")


run_fab_list = ast.literal_eval(RUN_FAB_LIST.get(PROCESS_STAGE))
IS_RUN = "TRUE" if FAB in run_fab_list else "FALSE"  
print(f"IS_RUN = {IS_RUN}")

config = Config()

workflow_time = {
    "START_TIME": START_TIME,
    "END_TIME": END_TIME,
    "JOB_NAME": JOB_NAME,
    "FAB": FAB,
    "PROCESS_STAGE": PROCESS_STAGE,
    "IS_RUN": IS_RUN
}
taskvalue = TaskValue()
taskvalue.set_args(key=config.taskKey, value=workflow_time)
