# Databricks notebook source
# 推定回遊人数（単位：人数）の計算

# E=MC^2を施す
# Covid-19における「人流から10日後RNA正規化値を予測する」処理と同様に
# 「O→D」因果量を「Dの時系列データにおける各時刻の値の2乗」で割る。
# →1日における「O→D」の人数を得る

# テストプレイ
# シュレディンガー方程式を時系列データに落とし込むとdeltaDの縮約？？？

# COMMAND ----------

# test_2_1_b
# 来訪者数量が一致できるように調整する
# parquetファイルで出力している
# devで言うところのココにある
# /Workspace/Users/hirako@adintedmp.onmicrosoft.com/custom_order/nomura_fudosan/dev/test_2_1_b/parquet/test_estimate_migration_number_hourly_parquet_日次出力

# COMMAND ----------
%pip install python-dotenv==1.0.1


# COMMAND ----------

import os
from os.path import join
from dotenv import load_dotenv

import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal

import pyspark as ps
from pyspark.sql import SparkSession

from UseCases.calc_estimate_migration_number import calc_estimate_migration_number
from _injector import original_EM

# COMMAND ----------
spark = SparkSession.builder\
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")\
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
            .config("spark.sql.shuffle.partitions", 100)\
            .config("spark.sql.dynamicPartitionPruning.enabled", True)\
            .getOrCreate()
            # '_started'と'_committed_'で始まるファイルを書き込まないように設定
            # '_SUCCESS'で始まるファイルを書き込まないように設定
            # パーティション数を調整する
            # 動的パーティションプルーニングの有効化

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
BASE_PATH = get_env_var("BASE_PATH")
WORK_PATH = BASE_PATH + get_env_var("WORK_PATH")

SPECIFIED_DATE     = get_widgets_var("SPECIFIED_DATE")
ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'] = get_widgets_var("ANALYSIS_OBJ")
PROJECT_NAME       = get_widgets_var('PROJECT_NAME')
BASE_PATH          = WORK_PATH + PROJECT_NAME + '/'
ESTIM_VISIT_NAME   = get_env_var("ESTIM_VISIT_NAME")
ESTIM_VISIT_D_PATH = get_env_var("ESTIM_VISIT_D_PATH")
ESTIM_VISIT_H_PATH = get_env_var("ESTIM_VISIT_H_PATH")
CAUSALITY_D_PATH   = get_env_var("CAUSALITY_D_PATH")
CAUSALITY_H_PATH   = get_env_var("CAUSALITY_H_PATH")
OUTPUT_PATH        = get_env_var("OUTPUT_PATH")
ENABLE_GROUP_MODE  = get_widgets_var("ENABLE_GROUP_MODE")

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

# 日付を設定する
date_aib, date_gps = set_date()
date = date_aib if ANALYSIS_OBJ == "AI_BEACON" else date_gps
print(ANALYSIS_OBJ)
print(date)

# COMMAND ----------
ENABLE_GROUP_MODE = True if ENABLE_GROUP_MODE.lower() in ['true', 't'] else False

calc_estimate_migration_number(original_EM(ANALYSIS_OBJ, BASE_PATH, 'daily',  ESTIM_VISIT_NAME, ESTIM_VISIT_D_PATH, CAUSALITY_D_PATH, OUTPUT_PATH, date, ENABLE_GROUP_MODE))