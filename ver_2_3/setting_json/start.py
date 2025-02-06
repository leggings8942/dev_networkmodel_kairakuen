# Databricks notebook source
# MAGIC %md
# MAGIC databricks上での動作用のjson scriptを生成する

# COMMAND ----------
import json
from typing import Literal
import pandas as pd

# COMMAND ----------
ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'] = dbutils.widgets.get("ANALYSIS_OBJ")
NETWORK_LIST         = dbutils.widgets.get("NETWORK_LIST")
PROJECT_NAME         = dbutils.widgets.get("PROJECT_NAME")
USE_MODEL_TYPE       = dbutils.widgets.get("USE_MODEL_TYPE")
SPECIFIED_START_DATE = dbutils.widgets.get("SPECIFIED_START_DATE")
SPECIFIED_END_DATE   = dbutils.widgets.get("SPECIFIED_END_DATE")
ENABLE_GROUP_MODE    = dbutils.widgets.get("ENABLE_GROUP_MODE")
TIME_INTERVAL        = dbutils.widgets.get("TIME_INTERVAL")

# COMMAND ----------
start_date = SPECIFIED_START_DATE
end_date   = SPECIFIED_END_DATE
day_list   = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date)]

# COMMAND ----------
json_list = [
    {
        "ANALYSIS_OBJ":      ANALYSIS_OBJ,
        "NETWORK_LIST":      NETWORK_LIST,
        "PROJECT_NAME":      PROJECT_NAME,
        "SPECIFIED_DATE":    date,
        "USE_MODEL_TYPE":    USE_MODEL_TYPE,
        "ENABLE_GROUP_MODE": ENABLE_GROUP_MODE,
        "TIME_INTERVAL":     TIME_INTERVAL,
    }
    for date in day_list
]
print(json_list[0],json_list[-1])

json_period = {
    "ANALYSIS_OBJ":      ANALYSIS_OBJ,
    "NETWORK_LIST":      NETWORK_LIST,
    "PROJECT_NAME":      PROJECT_NAME,
    "SPECIFIED_PERIOD":  day_list,
    "USE_MODEL_TYPE":    USE_MODEL_TYPE,
    "ENABLE_GROUP_MODE": ENABLE_GROUP_MODE,
}

# COMMAND ----------
target_json = json.dumps(json_list,   indent=4)
print(target_json)

period_json = json.dumps(json_period, indent=4)

# COMMAND ----------
dbutils.jobs.taskValues.set('target_json',   target_json)
dbutils.jobs.taskValues.set('target_period', period_json)