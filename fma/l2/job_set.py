# Databricks notebook source
import os
import json
import requests

from typing import Dict
from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType, DateType, IntegerType, DecimalType
from pyspark.sql.utils import AnalysisException

from master.config import Config
from util.deltatable import DeltaTableCRUD
from util.util import spark

# COMMAND ----------

DEPLOY_FILE_PATH = "../app_config/deploy_module.json"

workflow_path_dict = {
    "MODULE": "../app_config/fma_module.json",
    "LCMBEOL": "../app_config/fma_lcmbeol.json",
    "BRISAMODULE":"../app_config/fma_brisamodule.json",
}


with open(DEPLOY_FILE_PATH) as f:
    DEPLOY_JSON = json.load(f)

# COMMAND ----------

class JobMigration:
    def __init__(self, deploy_env: str, which_job: str) -> None:
        """
            :deploy_env : "QA" or "PRD"
            :which_job : "L4_L5_L6" or "L6_IMS"
        """
        self.deploy_env = deploy_env
        self.which_job = which_job
        self.deploy_json = DEPLOY_JSON[deploy_env]

        self.domain = self.deploy_json["DOMAIN"]
        self.token = self.deploy_json["TOKEN"]

    @property
    def src_job(self) -> None:
        path = workflow_path_dict[self.which_job]
        with open(path) as file:
            workflow_json = json.load(file)
        print(f"Load `{self.which_job}` src job structure.")
        return workflow_json

    def create_job(self) -> None:
        response = requests.post(
            url=f"https://{self.domain}/api/2.1/jobs/create",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.src_job
        )
        if response.status_code == 200:
            print(
                f"""Create {self.deploy_env} JOB\n JOB_NAME: {self.which_job}.""")
        else:
            print(
                f"""Error\nerror_code: {response.json()["error_code"]}\nerror_message: {response.json()["message"]}""")

    @property
    def revised_job(self) -> None:
        dest_job = self.src_job
        dest_job["job_id"] = self.target_job_id
        dest_job["settings"]["name"] = self.target_job_name
        dest_job["settings"]["git_source"]["git_branch"] = self.target_branch
        dest_job['settings']['tasks'] = []

        def fab2(stage: str):
            fab_dict = {
                'MODULE': ['K01','K06','L6K','M02','M11','S01','S02','S06','S11','S17'],
                'LCMBEOL': ['S01','S02'],
                'BRISAMODULE': ['S13','X13','Z0D','Z0H','Z0M','Z0W','Z1F','Z1L','Z22','Z31','Z40','Z45','Z94','ZF2','ZK7','ZKC','Z1E'],
            }
            print(stage)
            return fab_dict[stage]

        for stage in ['BRISAMODULE']:
            fab_list = fab2(stage)
            print(f"fab_list: {fab_list}")
            for fab in fab_list:
                
                fab_set_l2_arg = {
                    "task_key": f"{fab}_{stage.lower()}_set_l2_args",
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                    "notebook_path": "fma/l2/l2_set_job_args",
                    "base_parameters": {
                        "START_TIME": "",
                        "END_TIME": "",
                        "FAB": fab,
                        "JOB_NAME": f"{fab}_{stage}_F2C_H_DAX_FBK_FMA",
                        "PROCESS_STAGE": stage
                    },
                    "source": "GIT"
                    },
                    "job_cluster_key": "Job_cluster",
                    "max_retries": 3,
                    "min_retry_interval_millis": 30000,
                    "retry_on_timeout": False,
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                    },
                    "webhook_notifications": {}
                }

                master = {
                        "task_key": f"{fab}_{stage.lower()}_h_dax_fbk_fma",
                        "depends_on": [
                        {
                            "task_key": f"{fab}_{stage.lower()}_set_l2_args"
                        }
                        ],
                        "run_if": "ALL_SUCCESS",
                        "notebook_task": {
                        "notebook_path": f"fma/l2/main_{stage.lower()}",
                        "base_parameters": {
                            "TASK_KEY":f"{fab}_{stage.lower()}_set_l2_args"
                        },
                        "source": "GIT"
                        },
                        "job_cluster_key": "Job_cluster",
                        "max_retries": 3,
                        "min_retry_interval_millis": 30000,
                        "retry_on_timeout": False,
                        "timeout_seconds": 0,
                        "email_notifications": {},
                        "notification_settings": {
                        "no_alert_for_skipped_runs": False,
                        "no_alert_for_canceled_runs": False,
                        "alert_on_last_attempt": False
                        },
                        "webhook_notifications": {}
                    }
            
                # task append
                dest_job['settings']['tasks'].append(fab_set_l2_arg)
                dest_job['settings']['tasks'].append(master)  
        dest_job["new_settings"] = dest_job.pop("settings")
        # dest_job
        return dest_job 
            
    def reset_job(self, 
                  stage: str,
                  target_job_id: str,
                  target_job_name: str,
                  target_branch: str,
                  target_task_name: str = "set_l2_args") -> None:
        self.target_job_id = target_job_id
        self.target_job_name = target_job_name
        self.target_branch = target_branch
        self.target_task_name = target_task_name
        self.stage = stage
        # self.fab_list = fab(stage)

        response = requests.post(
            url=f"https://{self.domain}/api/2.1/jobs/reset",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.revised_job
        )
        if response.status_code == 200:
            print(
                f"""Reset {self.deploy_env} JOB\n\tJOB_NAME: {self.which_job}\n\tJOB_ID = {self.target_job_id}.""")
        else:
            print(
                f"""Error\nerror_code: {response.json()["error_code"]}\nerror_message: {response.json()["message"]}""")

# COMMAND ----------

env = "DEV"
job_id = 1027622322547226      
job_name = "FBK_FMA_BRISAMODULE_ODSToCentral"
branch = "FMA"
L2_L3 = JobMigration(deploy_env=env, which_job="BRISAMODULE")
L2_L3.reset_job(stage="BRISAMODULE", target_job_id=job_id, target_job_name=job_name, target_branch=branch)
