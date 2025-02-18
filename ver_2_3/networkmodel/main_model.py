# Databricks notebook source
# MAGIC %md
# MAGIC #### ver2.1からver2.2への変更内容
# MAGIC - 対象案件に関係なく、日別と1時間別を出力しておく
# MAGIC - 直行化撹乱項の出力を廃止
# MAGIC - 直交化インパルス応答関数（回遊2階層）の出力を廃止
# MAGIC - 直行化インパルス応答関数の１ショックの基準を１偏差から１単位へと変更
# MAGIC - データソースを以下の２つのテーブルから選択できるように変更
# MAGIC     - adinte.aibeacon_wifi_log
# MAGIC     - adinte_analyzed_data.gps_contact
# MAGIC - 出力パスと出力ファイルの変更
# MAGIC     - 日別・時間別の出力ファイルを生成するにあたり、できる限り出力ファイルを<br>
# MAGIC       まとめるように変更した。
# MAGIC     - 人気エリアの出力をsilerレイヤへと変更
# MAGIC     - 新規日別の出力ファイル<br>
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_causality.csv
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_centrality.csv
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_migration_3.csv
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_migration_4.csv
# MAGIC >       silver/{folder_name}/output/daily/{%Y-%m-%d}_人気エリア.csv
# MAGIC     - 新規時間別の出力ファイル<br>
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_causality.csv
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_centrality.csv
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_migration_3.csv
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_migration_4.csv
# MAGIC >       silver/{folder_name}/output/hourly/{%Y-%m-%d}_人気エリア.csv

# COMMAND ----------
%pip install python-dotenv==1.0.1
%pip install networkx==3.3

# COMMAND ----------

# インポート
import os
import sys
from os.path import join
from dotenv import load_dotenv

import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal

import pandas as pd

from model.networkmodel import networkmodel
from model._interface import UseInterface
from _injector import public_VAR, original_VAR, original_SVAR, original_NNVAR
from _injector import original_UL, original_DL


sys.setrecursionlimit(2000)

# COMMAND ----------
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

# COMMAND ----------

load_dotenv(join(os.getcwd(), '.env'))
# 設定ファイルパスとファイル名((TODO:configリストのパス先は要検討))
BASE_PATH           = get_env_var("BASE_PATH")
WORK_PATH           = BASE_PATH + get_env_var("WORK_PATH")

SPECIFIED_DATE      = get_widgets_var("SPECIFIED_DATE")
ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'] = get_widgets_var("ANALYSIS_OBJ")
PROJECT_NAME        = get_widgets_var('PROJECT_NAME')
BY1MIN_PATH         = get_env_var("BY1MIN_PATH")
INSTRUCT_PATH       = get_widgets_var('NETWORK_LIST')
USE_MODEL_TYPE      = get_widgets_var('USE_MODEL_TYPE')
ENABLE_GROUP_MODE   = get_widgets_var('ENABLE_GROUP_MODE')
TIME_INTERVAL       = get_widgets_var('TIME_INTERVAL')
MIGRATION_FLOOR_NUM = get_widgets_var('MIGRATION_FLOOR_NUM')

# COMMAND ----------

def set_date():
    if SPECIFIED_DATE.lower() in ['now', 'n']:
        dt = datetime.datetime.now()
        dt_aib = dt - relativedelta(days=1) + datetime.timedelta(hours=9)
        dt_gps = dt - relativedelta(days=8) + datetime.timedelta(hours=9)
        return dt_aib.strftime('%Y-%m-%d'), dt_gps.strftime('%Y-%m-%d')
    
    else:
        dt = datetime.datetime.strptime(SPECIFIED_DATE, '%Y-%m-%d')
        return dt.strftime('%Y-%m-%d'), dt.strftime('%Y-%m-%d')

# COMMAND ----------
if ENABLE_GROUP_MODE.lower() in ['true', 't']:
    ENABLE_GROUP_MODE = True
else:
    ENABLE_GROUP_MODE = False

if   USE_MODEL_TYPE.lower() in ['external', 'external library', 'external_library', 'el']:
    use_model = public_VAR()
elif USE_MODEL_TYPE.lower() in ['sparse',   'sparse model',     'sparse_model',     'sm']:
    use_model = original_SVAR()                                                                    # 使用する時系列解析モデル
else:
    use_model = original_NNVAR()                                                                   # 使用する時系列解析モデル
use_upload   = original_UL(ANALYSIS_OBJ, WORK_PATH + PROJECT_NAME)                                 # 使用するアップロードクラス
use_download = original_DL(ANALYSIS_OBJ, WORK_PATH + PROJECT_NAME, BY1MIN_PATH, ENABLE_GROUP_MODE) # 使用するダウンロードクラス
spec_comb    = UseInterface(use_model, use_upload, use_download)

# COMMAND ----------

# 日付を設定する
date_aib, date_gps = set_date()
date = date_aib if ANALYSIS_OBJ == "AI_BEACON" else date_gps
print(ANALYSIS_OBJ)
print(date)

# COMMAND ----------
nwm_conf = use_download.read_csv_file(INSTRUCT_PATH)
# print(nwm_conf.head(3))

if ANALYSIS_OBJ == "AI_BEACON":
    if not ENABLE_GROUP_MODE:
        # グループ化しない場合には unit id を使用する
        unique_id = 'unit_id'
    else:
        # グループ化する場合には network id を使用する
        unique_id = 'network_id'
        
    
    # 対象案件におけるunique_idのリストを取得する
    utid_list = sorted(nwm_conf[unique_id].drop_duplicates().to_list())
    utid_list = list(map(lambda x: str(x), utid_list))
    # print(utid_list)
    param_dict_nwm = {
        'date'         : date,
        'ueid_list'    : utid_list,
        'folder_name'  : PROJECT_NAME,
        'interval'     : TIME_INTERVAL,
        'migrate_num'  : MIGRATION_FLOOR_NUM,
    }
    
    # networkmodel処理
    networkmodel(spec_comb, param_dict_nwm)
else:
    if not ENABLE_GROUP_MODE:
        # グループ化しない場合には place id を使用する
        unique_id = 'place_id'
    else:
        # グループ化する場合には network id を使用する
        unique_id = 'network_id'
    
    
    # 対象案件におけるunique_idのリストを取得する
    peid_list = sorted(nwm_conf[unique_id].drop_duplicates().to_list())
    peid_list = list(map(lambda x: str(x), peid_list))
    # print(peid_list)
    param_dict_nwm = {
        'date'         : date,
        'ueid_list'    : peid_list,
        'folder_name'  : PROJECT_NAME,
        'interval'     : TIME_INTERVAL,
        'migrate_num'  : MIGRATION_FLOOR_NUM,
    }
    
    # networkmodel処理
    networkmodel(spec_comb, param_dict_nwm)