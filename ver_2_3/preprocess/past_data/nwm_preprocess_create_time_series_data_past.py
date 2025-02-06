# Databricks notebook source
# 3.時間粒度別に前処理を行う
# 時間粒度は、日別・1時間別・30分別・10分別・1分別
# 時間単位ごとにランダムmacアドレスユニーク処理を施す
# rssiを、70以下、75以下、80以下で別々に集計して格納する
# ファイル構造は「時間粒度」→「rssi」

# COMMAND ----------

from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta
import time
import os
import shutil

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when,expr,count,concat
from pyspark.sql import types as T
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

# '_started'と'_committed_'で始まるファイルを書き込まないように設定
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
# '_SUCCESS'で始まるファイルを書き込まないように設定
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
#パーティション数を増やす
spark.conf.set("spark.sql.shuffle.partitions",200)#2000

# COMMAND ----------

# MAGIC %run ../create_time_series_data

# COMMAND ----------

for i in range(1,3):
    # 日付を設定する(NOTE:datetime.now()なら日本時間になる)
    dt = datetime.now() + timedelta(hours=9) - relativedelta(days=i)
    # dt = date(2024,5,19) + timedelta(hours=9) - relativedelta(days=1)
    date = dt.strftime('%Y-%m-%d')
    print(date)
    # 生データ読み込み
    raw_data_path = f'dbfs:/mnt/adintedataexplorer_ml-medallion/dev/bronze/aibeacon_raw_data/'
    raw_data_date_path = raw_data_path + f'year={date[0:4]}/month={date[0:7]}/date={date}/*.parquet'
    # print(raw_data_date_path)
    df_raw_data = (spark.read\
                    .option('inferSchema', 'False')
                    .option('header', 'True')
                    .parquet(raw_data_date_path))
    df_raw_data_cache =  df_raw_data.cache()
    # 出力パス
    out_path = f'dbfs:/mnt/adintedataexplorer_ml-medallion/dev/silver/time_series_data/'
    # rssiでフィルタリングする
    for rssi in [80,75,70]:
        print(f'rssi={rssi}*****************')
        df_raw_rssi = df_raw_data_cache.filter(col('rssi') <= rssi).cache()
        # 時間帯粒度で処理する
        create_time_series_data_daily(df_raw_rssi,date,out_path+f'daily/rssi={rssi}/')
        create_time_series_data_hourly(df_raw_rssi,date,out_path+f'hourly/rssi={rssi}/')
        create_time_series_data_30min(df_raw_rssi,date,out_path+f'by30min/rssi={rssi}/')
        create_time_series_data_10min(df_raw_rssi,date,out_path+f'by10min/rssi={rssi}/')
        create_time_series_data_1min(df_raw_rssi,date,out_path+f'by1min/rssi={rssi}/')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


