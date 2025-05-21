# Databricks notebook source
import ast
import json
from datetime import datetime, time, timedelta

from util.taskvalue import TaskValue
from master.config import Config

# COMMAND ----------

F2C_START_TIME = dbutils.widgets.get("F2C_START_TIME")
F2C_END_TIME = dbutils.widgets.get("F2C_END_TIME")
JOB_NAME = dbutils.widgets.get("JOB_NAME")
ARRAY_RUN_FAB = dbutils.widgets.get("ARRAY_RUN_FAB")
FEOL_RUN_FAB = dbutils.widgets.get("FEOL_RUN_FAB")
BEOL_RUN_FAB = dbutils.widgets.get("BEOL_RUN_FAB")

print(f"F2C_START_TIME = {F2C_START_TIME}")
print(f"F2C_END_TIME = {F2C_END_TIME}")
print(f"JOB_NAME = {JOB_NAME}")
print(f"ARRAY_RUN_FAB = {ARRAY_RUN_FAB}")
print(f"FEOL_RUN_FAB = {FEOL_RUN_FAB}")
print(f"BEOL_RUN_FAB = {BEOL_RUN_FAB}")


config = Config()

workflow_time = {
    "START_TIME": F2C_START_TIME,
    "END_TIME": F2C_END_TIME,
    "JOB_NAME": JOB_NAME,
    "ARRAY_RUN_FAB": ast.literal_eval(ARRAY_RUN_FAB),
    "FEOL_RUN_FAB": ast.literal_eval(FEOL_RUN_FAB),
    "BEOL_RUN_FAB": ast.literal_eval(BEOL_RUN_FAB),
}
taskvalue = TaskValue()
taskvalue.set_args(key=config.taskKey, value=workflow_time)

