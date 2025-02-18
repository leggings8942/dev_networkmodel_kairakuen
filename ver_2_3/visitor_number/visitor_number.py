# Databricks notebook source
%pip install python-dotenv==1.0.1
%pip install xxhash==3.5.0

# COMMAND ----------
import os
from os.path import join
from dotenv import load_dotenv
from typing import Literal

# COMMAND ----------
load_dotenv(join(os.getcwd(), '.env'))
def get_env_var(key:str) -> str:
    var = os.environ.get(key)
    if var is None:
        raise ValueError(f"環境変数'{key}' が設定されていません")
    return var

def get_widgets_var(key:str) -> str:
    try:
        var = dbutils.widgets.get(key)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"dbutils.widgets経由での環境変数'{key}' の取得に失敗しました")
        print(f"os.environ経由での環境変数'{key}' の取得に切り替えます。")
        var = os.environ.get(key)
    finally:
        if var is None:
            raise ValueError(f"環境変数'{key}' が設定されていません")
    return var

def run_widgets_notebook(filename:str, timeout:int, args:map) -> str:
    try:
        s = dbutils.notebook.run(filename, timeout, args)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"dbutils.notebook経由でのnotebook'{filename}' の実行に失敗しました")
        raise RuntimeError("execution failed")
    return s

# COMMAND ----------
load_dotenv(join(os.getcwd(), '.env'))
BASE_PATH         = get_env_var("BASE_PATH")
WORK_PATH         = BASE_PATH + get_env_var("WORK_PATH")
AIBEACON_PATH     = get_env_var("AIBEACON_PATH")
GPS_DATA_PATH     = get_env_var("GPS_DATA_PATH")

ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'] = get_widgets_var("ANALYSIS_OBJ")
NETWORK_LIST      = get_widgets_var("NETWORK_LIST")
PROJECT_NAME      = get_widgets_var("PROJECT_NAME")
SPECIFIED_DATE    = get_widgets_var("SPECIFIED_DATE")
ENABLE_GROUP_MODE = get_widgets_var("ENABLE_GROUP_MODE")
ARGS = {
    "WORK_PATH" :        WORK_PATH,
    "AIBEACON_PATH":     AIBEACON_PATH,
    "GPS_DATA_PATH":     GPS_DATA_PATH,
    "ANALYSIS_OBJ":      ANALYSIS_OBJ,
    "NETWORK_LIST":      NETWORK_LIST,
    "PROJECT_NAME":      PROJECT_NAME,
    "SPECIFIED_DATE":    SPECIFIED_DATE,
    "ENABLE_GROUP_MODE": ENABLE_GROUP_MODE,
}

if ANALYSIS_OBJ == "AI_BEACON":
    run_widgets_notebook('visitor_by_optmz4', 0, ARGS)
    
else:
    run_widgets_notebook('visitor_by_multi_regression_on_gps', 0, ARGS)
    
# 日別の処理
# ・AI Beacon
# 　入力： preprocessのパケット
# 　計算モデル： optmz4
# 　出力： csv: user_id, place_id, unit_id, date, visitor_number
# ・GPS Data
# 　入力： adinte_analyzed_data.gps_contact
# 　計算モデル： on_gps
# 　出力： csv: user_id, place_id, date, visitor_number

# 時間別の処理
# ・AI Beacon
# 　入力： preprocessのパケット
# 　計算モデル： optmz4
# 　出力： csv: user_id, place_id, unit_id, date, hour, visitor_number
# ・GPS Data
# 　入力： adinte_analyzed_data.gps_contact
# 　計算モデル： on_gps
# 　出力： csv: user_id, place_id, date, hour, visitor_number