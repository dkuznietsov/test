# Databricks notebook source
import ast
import json
from datetime import datetime, time, timedelta

from util.taskvalue import TaskValue
from test.config import Config

# COMMAND ----------

START_TIME = dbutils.widgets.get("START_TIME")
END_TIME = dbutils.widgets.get("END_TIME")
JOB_NAME = dbutils.widgets.get("JOB_NAME")
BRISA_RUN_FAB = dbutils.widgets.get(f"BRISA_RUN_FAB")
EDI_BEOL_RUN_FAB = dbutils.widgets.get(f"EDI_BEOL_RUN_FAB")
EDI_RUN_FAB = dbutils.widgets.get(f"EDI_RUN_FAB")
LCM_BEOL_RUN_FAB = dbutils.widgets.get(f"LCM_BEOL_RUN_FAB")
MODULE_RUN_FAB = dbutils.widgets.get(f"MODULE_RUN_FAB")


print(f"START_TIME = {START_TIME}")
print(f"END_TIME = {END_TIME}")
print(f"JOB_NAME = {JOB_NAME}")


config = Config()

workflow_time = {
    "START_TIME": START_TIME,
    "END_TIME": END_TIME,
    "JOB_NAME": JOB_NAME,
    "BRISA_RUN_FAB": ast.literal_eval(BRISA_RUN_FAB),
    "EDI_BEOL_RUN_FAB": ast.literal_eval(EDI_BEOL_RUN_FAB),
    "EDI_RUN_FAB": ast.literal_eval(EDI_RUN_FAB),
    "LCM_BEOL_RUN_FAB": ast.literal_eval(LCM_BEOL_RUN_FAB),
    "MODULE_RUN_FAB": ast.literal_eval(MODULE_RUN_FAB),
}
taskvalue = TaskValue()
taskvalue.set_args(key=config.taskKey, value=workflow_time)

