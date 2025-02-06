# Databricks notebook source
# MAGIC %md
# MAGIC #### ver2.1からver2.2への変更内容
# MAGIC - 対象案件ごとに設定されたRSSIの値を基準値として使用する
# MAGIC - RAWデータの中間ファイルの出力を廃止
# MAGIC - データソースを以下の２つのテーブルから選択できるように変更
# MAGIC     - adinte.aibeacon_wifi_log
# MAGIC     - adinte_analyzed_data.gps_contact
# MAGIC - 出力パスと出力ファイルの変更
# MAGIC     - 日別・1時間別・30min別・10min別・1min別の出力ファイルを生成するにあたり、できる限り出力ファイルを<br>
# MAGIC       まとめるように変更した。
# MAGIC     - 各種時間別の出力ファイル<br>
# MAGIC >       silver/{folder_name}/preprocess/daily/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/hourly/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/by30min/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/by10min/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/by1min/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet

# COMMAND ----------
%pip install python-dotenv==1.0.1
%pip install tqdm==4.67.1

# COMMAND ----------
import os
from os.path import join
from dotenv import load_dotenv
import json
import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal
from tqdm.auto import tqdm

import pyspark as ps
from pyspark import StorageLevel
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col
import pyspark.sql.functions as F

from IO_control.import_file import read_csv_file
from UseCases.create_ai_beacon_data import create_ai_beacon_data
from UseCases.create_gps_data import create_gps_data
from _injector import original_UL


# COMMAND ----------
spark = SparkSession.builder\
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")\
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
            .config("spark.sql.adaptive.enabled", True)\
            .config("spark.sql.dynamicPartitionPruning.enabled", True)\
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1MB")\
            .getOrCreate()
            # '_started'と'_committed_'で始まるファイルを書き込まないように設定
            # '_SUCCESS'で始まるファイルを書き込まないように設定
            # AQE(Adaptive Query Execution)の有効化
            # 動的パーティションプルーニングの有効化
            # シャッフル後の1パーティションあたりの最小サイズを指定
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

def get_taskValues_json(taskKey:str, key:str) -> dict:
    try:
        task_json = dbutils.jobs.taskValues.get(taskKey=taskKey, key=key)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"dbutils.jobs.taskValues経由での環境変数'{taskKey}.{key}' の取得に失敗しました")
        print(f"dbutils.widgets経由での環境変数'{key}' の取得に切り替えます。")
        task_dict = {}
        task_dict["ANALYSIS_OBJ"]      = get_widgets_var("ANALYSIS_OBJ")
        task_dict["PROJECT_NAME"]      = get_widgets_var("PROJECT_NAME")
        task_dict["NETWORK_LIST"]      = get_widgets_var("NETWORK_LIST")
        task_dict["SPECIFIED_PERIOD"]  = [get_widgets_var("SPECIFIED_DATE")]
        task_dict["ENABLE_GROUP_MODE"] = get_widgets_var("ENABLE_GROUP_MODE")
        # task_dict["ENABLE_AIB_ROW"]    = get_widgets_var("ENABLE_AIB_ROW")
    else:
        task_dict = json.loads(task_json)
    return task_dict

# COMMAND ----------

load_dotenv(join(os.getcwd(), '.env'))
# 設定ファイルパスとファイル名((TODO:configリストのパス先は要検討))
SETTING_TASK_NAME = get_env_var("SETTING_TASK_NAME")
BASE_PATH         = get_env_var("BASE_PATH")
WORK_PATH         = BASE_PATH + get_env_var("WORK_PATH")

period_json       = get_taskValues_json(SETTING_TASK_NAME, 'target_period')
SPECIFIED_PERIOD  = period_json["SPECIFIED_PERIOD"]
ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'] = period_json["ANALYSIS_OBJ"]
PROJECT_NAME      = period_json["PROJECT_NAME"]
INSTRUCT_PATH     = WORK_PATH + PROJECT_NAME + "/" + period_json["NETWORK_LIST"]
AIBEACON_PATH     = get_env_var("AIBEACON_PATH")
GPS_DATA_PATH     = get_env_var("GPS_DATA_PATH")
ENABLE_GROUP_MODE = period_json["ENABLE_GROUP_MODE"]
# RAW_AIB_PATH      = get_env_var("RAW_AIB_PATH")
# ENABLE_AIB_ROW    = period_json["ENABLE_AIB_ROW"]
RAW_AIB_PATH      = ''
ENABLE_AIB_ROW    = 'False'

# COMMAND ----------
def set_date():
    if (len(SPECIFIED_PERIOD) == 1) and (SPECIFIED_PERIOD[0].lower() in ['now', 'n']):
        dt = datetime.datetime.now()
        dt_aib = dt - relativedelta(days=1) + datetime.timedelta(hours=9)
        dt_gps = dt - relativedelta(days=8) + datetime.timedelta(hours=9)
        return [dt_aib.strftime('%Y-%m-%d')], [dt_gps.strftime('%Y-%m-%d')]
    
    else:
        dt = [datetime.datetime.strptime(sp_date, '%Y-%m-%d').strftime('%Y-%m-%d') for sp_date in SPECIFIED_PERIOD]
        return dt, dt

# COMMAND ----------
if ANALYSIS_OBJ == "AI_BEACON":
    need_col = ['folder_name', 'user_id', 'place_id', 'network_id', 'unit_id', 'rssi_fx']
else:
    need_col = ['folder_name', 'user_id', 'place_id', 'network_id']

if ENABLE_GROUP_MODE.lower() in ['true', 't']:
    ENABLE_GROUP_MODE = True
else:
    ENABLE_GROUP_MODE = False

if ENABLE_AIB_ROW.lower() in ['true', 't']:
    ENABLE_AIB_ROW = True
else:
    ENABLE_AIB_ROW = False

# 設定ファイルから生データを取得する
nwm_conf = read_csv_file(INSTRUCT_PATH)
nwm_conf = nwm_conf.select(need_col)
try:
    nwm_conf.persist(StorageLevel.MEMORY_ONLY)
    nwm_conf.display()
    correspFlag = True
except Exception as e:
    print(e)
    print('データフレームの永続化に対応していません')
    correspFlag = False

# COMMAND ----------

# 日付を設定する
date_aib, date_gps = set_date()
dates = date_aib if ANALYSIS_OBJ == "AI_BEACON" else date_gps
print(dates)

# COMMAND ----------
if ANALYSIS_OBJ == "AI_BEACON":
    nwm_list = sorted(row['unit_id'] for row in nwm_conf.select('unit_id').dropDuplicates().collect())
    nwm_list = list(map(lambda x: str(x), nwm_list))
    
    if ENABLE_AIB_ROW == False: # databricksのテーブルから取得する
        # 生データと設定ファイルとを突合する
        df_raw_data = spark.table(AIBEACON_PATH)\
                            .filter(col('date').isin(dates))\
                            .filter(col('randomized') == '1')\
                            .select(['date', 'datetime', 'unit_id', 'aibeaconid', 'rssi'])\
                            .filter(col('unit_id').isin(nwm_list))\
                            .join(nwm_conf, on='unit_id', how='inner')\
                            .filter(col('rssi') <= col('rssi_fx'))\
                            .select(['date', 'datetime', 'unit_id', 'aibeaconid', 'folder_name', 'user_id', 'place_id', 'network_id'])
                            # 指定の日付 かつ
                            # ランダムMACアドレス かつ
                            # 指定の列のみを抽出
                            # unit_idが一致している かつ
                            # rssi <= rssi_fx かつ
                            # 指定の列のみを抽出
    else: # databricksのテーブルへ格納する前のデータを取得する
        df_schema  = types.StructType([
                        types.StructField('date',        types.DateType(),      False),
                        types.StructField('datetime',    types.TimestampType(), False),
                        types.StructField('unit_id',     types.StringType(),    False),
                        types.StructField('aibeaconid',  types.StringType(),    False),
                        types.StructField('folder_name', types.StringType(),    False),
                        types.StructField('user_id',     types.StringType(),    False),
                        types.StructField('place_id',    types.StringType(),    False),
                        types.StructField('network_id',  types.StringType(),    False),
                    ])
        df_raw_data = spark.createDataFrame([], df_schema)
        # 生データと設定ファイルとを突合する
        for target in dates:
            date = datetime.datetime.strptime(target, '%Y-%m-%d').strftime('%Y%m%d')
            for unit_id in nwm_list:
                ACCESS_PATH = RAW_AIB_PATH + date + '/' + f'{unit_id}_{date}.csv.gz'
                df_tmp = spark.read.csv(RAW_AIB_PATH)\
                                    .filter(col('random') == '1')\
                                    .withColmnRenamed('mac', 'aibeaconid')\
                                    .withColmnRenamed('timestamp', 'datetime')\
                                    .withColmn('datetime', F.from_unixtime('datetime'))\
                                    .withColmn('date', F.to_date('datetime'))\
                                    .withColmn('unit_id', F.lit(unit_id))\
                                    .select(['date', 'datetime', 'unit_id', 'aibeaconid', 'rssi'])\
                                    .join(nwm_conf, on='unit_id', how='inner')\
                                    .filter(col('rssi') <= col('rssi_fx'))\
                                    .select(['date', 'datetime', 'unit_id', 'aibeaconid', 'folder_name', 'user_id', 'place_id', 'network_id'])
                                    # ランダムMACアドレス指定
                                    # 各列の名称変更
                                    # 時刻カラムの追加
                                    # 指定の列のみを抽出
                                    # unit_idが一致している かつ
                                    # rssi <= rssi_fx かつ
                                    # 指定の列のみを抽出
                df_raw_data = df_raw_data.union(df_tmp)
                
        
    
    # 各列の型の明示
    df_raw_data = df_raw_data\
                        .withColumn('date',        col('date').cast(       types.StringType()))\
                        .withColumn('datetime',    col('datetime').cast(   types.StringType()))\
                        .withColumn('unit_id',     col('unit_id').cast(    types.StringType()))\
                        .withColumn('aibeaconid',  col('aibeaconid').cast( types.StringType()))\
                        .withColumn('folder_name', col('folder_name').cast(types.StringType()))\
                        .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                        .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                        .withColumn('network_id',  col('network_id').cast( types.StringType()))
else:
    nwm_list = sorted(row['place_id'] for row in nwm_conf.select('place_id').dropDuplicates().collect())
    nwm_list = list(map(lambda x: str(x), nwm_list))
    
    # 生データと設定ファイルとを突合する
    df_raw_data = spark.table(GPS_DATA_PATH)\
                        .filter(col('date').isin(dates))\
                        .select(['date', 'datetime', 'adid', 'place_id'])\
                        .filter(col('place_id').isin(nwm_list))\
                        .join(nwm_conf, on='place_id', how='inner')\
                        .select(['date', 'datetime', 'adid', 'place_id', 'network_id'])
                        # 指定の日付 かつ
                        # 指定の列のみを抽出
                        # place_idが一致している かつ
                        # 指定の列のみを抽出
    # 各列の型の明示
    df_raw_data = df_raw_data\
                        .withColumn('date',       col('date').cast(      types.StringType()))\
                        .withColumn('datetime',   col('datetime').cast(  types.StringType()))\
                        .withColumn('adid',       col('adid').cast(      types.StringType()))\
                        .withColumn('place_id',   col('place_id').cast(  types.StringType()))\
                        .withColumn('network_id', col('network_id').cast(types.StringType()))
try:
    df_raw_data.persist(StorageLevel.MEMORY_ONLY)
    df_raw_data.display()
    correspFlag = True
except Exception as e:
    print(e)
    print('データフレームの永続化に対応していません')
    correspFlag = False

# COMMAND ----------
if ANALYSIS_OBJ == "AI_BEACON":
    for date in tqdm(dates):
        print(date)
        df_tmp = df_raw_data.filter(col('date') == date)
        
        # 時間帯粒度で処理する
        path = WORK_PATH + PROJECT_NAME + '/preprocess/'
        use_upload = original_UL(path, date)
        create_ai_beacon_data(use_upload, df_tmp, ENABLE_GROUP_MODE)
else:
    for date in tqdm(dates):
        print(date)
        df_tmp = df_raw_data.filter(col('date') == date)
        
        # 時間帯粒度で処理する
        path = WORK_PATH + PROJECT_NAME + '/preprocess/'
        use_upload = original_UL(path, date)
        create_gps_data(use_upload, df_tmp, ENABLE_GROUP_MODE)

# COMMAND ----------
# 占有リソースの解放
if correspFlag:
    nwm_conf.unpersist()
    df_raw_data.unpersist()