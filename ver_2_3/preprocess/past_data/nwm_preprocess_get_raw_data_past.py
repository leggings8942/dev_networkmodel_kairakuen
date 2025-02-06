# Databricks notebook source
# 生データを取得する（毎日、午前7時が望ましい）
# 設定ファイルから生データを取得する
# 必要な情報：unit_id
# ランダムmacアドレスのみ取得する
# 用途に合わせるため、rssi<=80（15m範囲）で取得する
# 日付でパーティションする
# parquetファイルで出力する

# COMMAND ----------

from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta
import time
import os
import shutil

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when
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

# 設定ファイルから生データを取得する
input_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/NWM/OPTMZ4/input/*.csv'
df_config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(input_path))
df_config.display()

# COMMAND ----------

# 設定ファイルから生データを取得する
input_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/NWM/来訪者数推定/input/*.csv'
df_config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(input_path))
df_config.display()

# COMMAND ----------

# 設定ファイルから生データを取得する
input_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/network_list_by_projects.csv'
df_config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(input_path))
df_config.display()

# COMMAND ----------

# MAGIC %md
# MAGIC いったん、１つの設定ファイルから作成する
# MAGIC 後で、設定ファイルのパス先などを変更する

# COMMAND ----------

# 設定ファイル取得する
input_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/network_list_by_projects.csv'
df_config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(input_path))\
        .select(['folder_name','user_id','place_id','unit_id'])
df_config.display()

# COMMAND ----------

for i in range(1,3):
    # 日付を設定する(NOTE:datetime.now()なら日本時間になる)
    dt = datetime.now() + timedelta(hours=9) - relativedelta(days=i)
    date = dt.strftime('%Y%m%d')
    print(date)
    # 生データと設定ファイルとを突合する
    aib_table = 'adinte.aibeacon_wifi_log'
    df_raw_data = spark.table(aib_table)\
                    .filter(col('src')==date)\
                    .filter(col('randomized') == '1')\
                    .join(df_config,on='unit_id',how='inner')\
                    .filter(col('rssi')<=80)\
                    .select(['date','datetime','unit_id','aibeaconid','rssi','folder_name','user_id','place_id'])
    # df_raw_data.display()
    # 出力する
    out_path = f'dbfs:/mnt/adintedataexplorer_ml-medallion/dev/bronze/aibeacon_raw_data/'
    out_date_path = out_path + f'year={date[0:4]}/month={date[0:4]}-{date[4:6]}/date={date[0:4]}-{date[4:6]}-{date[6:8]}'
    # print(out_date_path)
    # 出力
    df_raw_data\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .parquet(out_date_path)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


