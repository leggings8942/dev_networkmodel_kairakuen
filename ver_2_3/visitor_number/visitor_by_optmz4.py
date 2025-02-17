# Databricks notebook source
# version1.1
# ver1.1:RSSIでフィルタ・「aibeaconid・yyyymmdd:hh」でユニーク

# COMMAND ----------
WORK_PATH         = dbutils.widgets.get("WORK_PATH")
AIBEACON_PATH     = dbutils.widgets.get("AIBEACON_PATH")
PROJECT_NAME      = dbutils.widgets.get("PROJECT_NAME")
SPECIFIED_DATE    = dbutils.widgets.get("SPECIFIED_DATE")
NETWORK_LIST      = dbutils.widgets.get("NETWORK_LIST")
ENABLE_GROUP_MODE = dbutils.widgets.get("ENABLE_GROUP_MODE")

# COMMAND ----------

import csv
from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta,MO
from functools import reduce
import math
import os
import pickle
import random

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
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

# 日付を設定する
if ENABLE_GROUP_MODE.lower() not in ['true', 't']:
    unique_id = 'unit_id'
else:
    unique_id = 'network_id'

if SPECIFIED_DATE.lower() in ['now', 'n']:
    dt = datetime.now()
    dt = dt - timedelta(days=1) + timedelta(hours=9)
else:
    dt = datetime.strptime(SPECIFIED_DATE, '%Y-%m-%d')

date,date_yymmdd = dt.strftime('%Y-%m-%d'),dt.strftime('%Y%m%d')
print(date)

# COMMAND ----------

PATH    = f'dbfs:{WORK_PATH + PROJECT_NAME}/'
PD_PATH = f'/dbfs{WORK_PATH + PROJECT_NAME}/'
DATE_PATH = f'year={date[0:4]}/month={date[0:7]}/date={date}/'
# powerbi/basic_stats/tana_fes/input/network_list_by_tana_fes.csv
# 設定ファイル読み込みパス
config_path = PATH + NETWORK_LIST
# 前処理データパス
count_path  = PATH+f'preprocess/'
# 中間生成物出力パス（日次）
intermediate_path_daily = PD_PATH+f'intermediate/daily/'
# 中間生成物出力パス（1時間）
intermediate_path_hourly = PD_PATH+f'intermediate/hourly/'

# 出力パス（日次）
csv_dir_path_daily = PD_PATH+f'output/来訪者数推定_OPTMZ4/daily/'
# 出力パス（1時間）
csv_dir_path_hourly = PD_PATH+f'output/来訪者数推定_OPTMZ4/hourly/'

# COMMAND ----------

table_aib_law = AIBEACON_PATH

# COMMAND ----------

a1 = 0.00784075050523477
seed_value = 42
random_rate_ul = 1.4
random_rate_ll = 0.6
random_val_avg = 1.7

# COMMAND ----------

# 設定ファイル読み込み
config = (spark.read\
            .option('inferSchema', 'False')
            .option('header', 'True')
            .csv(config_path)\
        )
config.display()


# COMMAND ----------

# 日別の算出

# COMMAND ----------

# 日別ユニーク計測数を取得する
count_path_daily = count_path+f'daily/{DATE_PATH}'+'*.parquet'
df_aib_count = spark.read\
            .option('inferSchema', 'False')\
            .option('header', 'True')\
            .parquet(count_path_daily)
# df_aib_count.display()
#乱数適用
df_aib_estimated = df_aib_count\
                .withColumn('a1_coef', F.lit(a1))\
                .withColumn('random_val_avg', F.lit(random_val_avg))\
                .withColumn('random_rate_ul', F.lit(random_rate_ul))\
                .withColumn('random_rate_ll', F.lit(random_rate_ll))\
                .withColumn('free_degree', random_val_avg*(F.rand(seed_value)* (random_rate_ul - random_rate_ll) + random_rate_ll))\
                .withColumn('a1_rand_coef', F.col('a1_coef')*F.col('random_val_avg')*F.col('free_degree'))\
                .withColumn('visitor_number_long', F.col('daily_count')*F.col('a1_rand_coef'))\
                .withColumn('visitor_number', F.col('visitor_number_long').cast(IntegerType()))\
                .sort(['user_id','place_id',unique_id])
df_aib_estimated.display()

# COMMAND ----------

# 日別の中間生成物

# COMMAND ----------

# 出力
# out_intermediate_path_daily = intermediate_path_daily + f'daily/'
filename = f'{date}_optmz4.csv'
# パス先がなければディレクトリの作成
if not os.path.isdir(intermediate_path_daily): 
    os.makedirs(intermediate_path_daily,exist_ok=True)
# csv出力
df_output_daily = df_aib_estimated\
                    .select(['user_id','place_id',unique_id,'date','a1_rand_coef','visitor_number'])\
                    .toPandas()
df_output_daily.set_index('user_id')\
    .to_csv(intermediate_path_daily+filename)



# COMMAND ----------

# 日別の出力

# COMMAND ----------

# 出力
out_path_daily = csv_dir_path_daily + DATE_PATH
filename = f'{date}_optmz4.csv'
# パス先がなければディレクトリの作成
if not os.path.isdir(out_path_daily): 
    os.makedirs(out_path_daily,exist_ok=True)
# csv出力
df_output_daily.set_index('user_id')\
    .to_csv(out_path_daily+filename)

# COMMAND ----------



# COMMAND ----------

# 1時間別ユニーク計測数を取得する
count_path_hourly = count_path +f'hourly/{DATE_PATH}'+'*.parquet'
df_aib_count_hourly = spark.read\
            .option('inferSchema', 'False')\
            .option('header', 'True')\
            .parquet(count_path_hourly)
# df_aib_count_hourly.display()
# 係数を掛ける
df_with_coef=df_aib_count_hourly\
                .join(F.broadcast(df_aib_estimated), ['user_id','place_id',unique_id,'date'], how='inner')\
                .withColumn('visitor_number_long', F.col('hourly_count')*F.col('a1_rand_coef'))\
                .withColumn('visitor_number', F.col('visitor_number_long').cast(IntegerType()))
df_with_coef.display()


# COMMAND ----------

# 1時間別の中間生成物

# COMMAND ----------

filename = f'{date}_optmz4.csv'
# パス先がなければディレクトリの作成
if not os.path.isdir(intermediate_path_hourly): 
    os.makedirs(intermediate_path_hourly,exist_ok=True)
# csv出力
df_output_hourly = df_with_coef\
                    .select(['user_id','place_id',unique_id,'date','hour','a1_rand_coef','visitor_number'])\
                    .toPandas()
df_output_hourly.set_index('user_id')\
    .to_csv(intermediate_path_hourly+filename)


# COMMAND ----------

# 1時間別の出力

# COMMAND ----------

# 出力
out_path_hourly = csv_dir_path_hourly + DATE_PATH
filename = f'{date}_optmz4.csv'
# パス先がなければディレクトリの作成
if not os.path.isdir(out_path_hourly): 
    os.makedirs(out_path_hourly,exist_ok=True)
# csv出力
df_output_hourly.set_index('user_id')\
    .to_csv(out_path_hourly+filename)


# COMMAND ----------


