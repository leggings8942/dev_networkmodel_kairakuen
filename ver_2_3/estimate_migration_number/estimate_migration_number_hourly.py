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

import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta,MO

import numpy as np
import pandas as pd
from pyspark.sql.functions import ceil,col,lit,when,expr,concat
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

# import networkx as nx
# from statsmodels.tsa.api import VAR

# COMMAND ----------

# '_started'と'_committed_'で始まるファイルを書き込まないように設定
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
# '_SUCCESS'で始まるファイルを書き込まないように設定
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
#パーティション数を増やす
spark.conf.set("spark.sql.shuffle.partitions",200)#2000

# COMMAND ----------

# TIME_SERIES_DATA_PATH = '/dbfs/mnt/adintedataexplorer_topicmodel/nwm_test/time_series_data/'
ESTIMATE_VISITOR_PATH = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/NWM/OPTMZ4/'
CAUSALITY_PATH = 'dbfs:/mnt/adintedataexplorer_ml-medallion/dev/silver/migration_4/'
OUTPUT_PATH = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/NWM/推定回遊人数/'
PROCESS_PATH = ''

# 設定ファイルパスとファイル名((TODO:configリストのパス先は要検討))
AIB_NWM_LIST  = '/dbfs/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/network_list_by_projects.csv'


# COMMAND ----------

def set_date():
    dt = datetime.now()
    # dt = datetime.date(2024,9,12)
    dt_aib = dt + timedelta(hours=9) - relativedelta(days=1)
    dt_gps = dt + timedelta(hours=9) - relativedelta(days=8)
    return dt_aib.strftime('%Y-%m-%d'),dt_gps.strftime('%Y-%m-%d')

# COMMAND ----------

def read_config_file(aib_list):
    aib_nwm_df  = pd.read_csv(aib_list,index_col=0,dtype={'place_id':str})
    conf_nwm_df = aib_nwm_df[['folder_name','area']].drop_duplicates()
    return aib_nwm_df,conf_nwm_df

# COMMAND ----------

# 2つの設定ファイルをfolder_nameで抽出する
def filter_folder_name(aib_df,conf_df,folder,area):
    aib_nwm = aib_df.query('folder_name == @folder')\
                    .query('area == @area')
    conf_nwm = conf_df.query('folder_name == @folder')\
                    .query('area == @area')
    return aib_nwm,conf_nwm

# COMMAND ----------

# 4階層

# COMMAND ----------

# hourly

# 日付を設定する
date_aib,date_gps = set_date()
# date_aib,date_gps = set_date_past(i)
date = date_aib
print(date_aib,date_gps)
# 設定ファイルを読み込む
aib_nwm_df,conf_nwm_df = read_config_file(AIB_NWM_LIST)

# folder_name,area別に処理を行う
for index,row in conf_nwm_df.iterrows():
    print(index)
    project = index
    folder_name = row['folder_name']
    area = row['area']
    # 2つの設定ファイルをfolder_nameで抽出する
    aib_nwm,conf_nwm = filter_folder_name(\
        aib_nwm_df,conf_nwm_df,folder_name,area)

    # 1時間単位集計の推定来訪者数を取得(E=MC^2におけるCに相当する部分)
    conf_file_name = ESTIMATE_VISITOR_PATH + 'input/aibeacon_list_ver1-1.csv'
    df_input_config = (spark.read\
            .option('inferSchema', 'False')
            .option('header', 'True')
            .csv(conf_file_name))\
            .filter(col('project')==project)
    # df_input_config.display()
    # TODO:集計していないプロジェクトは、一旦取り除く
    if df_input_config.count() == 0:
        continue

    file_name = ESTIMATE_VISITOR_PATH + f'output/hourly/year={date[0:4]}/month={date[0:7]}/date={date}/'

    df_visitor_count = (spark.read\
            .option('inferSchema', 'False')
            .option('header', 'True')
            .csv(file_name))\
            .join(df_input_config,on='group_id',how='inner')\
            .select(['date','hour','unit_id','visitor_number'])\
            .withColumn('ORIGIN',col('unit_id'))\
            .withColumn('DESTINATION',col('unit_id'))
    # df_visitor_count.display()

    # 各値を２乗する(Covid-19におけるRNAに合わせ逆数をとる)
    df_c_value = df_visitor_count\
                    .withColumn('C-value',\
                        when(col('visitor_number')>0,1/col('visitor_number')**2)\
                            .otherwise(lit(1)))

    # ４階層因果量データ読み込み
    out_causality_path = CAUSALITY_PATH+f'/daily/{folder_name}/{area}/{date}_migration_4.csv'
    df_causality = (spark.read\
            .option('inferSchema', 'False')
            .option('header', 'True')
            .csv(out_causality_path))\
            .drop('_c0')
    # df_causality.display()
    if df_causality.count() == 0:
        continue

    # 「O→D」因果量を「Dの時系列データにおける各時刻の値の2乗」で割る。(E=因果量,M=移動人数,C=推定人数)
    # →→→時間単位における「O→D」の人数を得る
    df_move_val = df_causality\
                    .join(df_c_value.select(['date','hour','DESTINATION','C-value']),on=['date','DESTINATION'],how='inner')\
                    .withColumn('move_val',col('移動影響量_VIA_2')/col('C-value'))
    # df_move_val.display()

    # 「move_val」をORIGIN基準で割合にして「move_pop」にする
    df_move_val_total = df_move_val\
                            .select(['ORIGIN','date','hour','move_val'])\
                            .groupBy(['date','hour','ORIGIN'])\
                            .sum()
    # df_move_val_total.display()
    df_move_pop = df_move_val\
                    .join(df_visitor_count.select(['date','hour','ORIGIN','visitor_number']),on=['date','hour','ORIGIN'],how='inner')\
                    .join(df_move_val_total,on=['date','hour','ORIGIN'],how='inner')\
                    .withColumn('move_pop',col('visitor_number')*(col('move_val')/col('sum(move_val)')))
#     df_move_pop.display()
    # 出力
    out_estimate_migration_number_path = OUTPUT_PATH + f'4階層/hourly/{folder_name}/{area}/{date[0:7]}/{date}/'
    df_move_pop\
        .select(['date','hour','ORIGIN','VIA_1','VIA_2','DESTINATION','move_pop'])\
        .sort(['date','hour','ORIGIN','VIA_1','VIA_2','DESTINATION'])\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .parquet(out_estimate_migration_number_path)

    # # テスト
    # df_move_pop\
    #     .select(['date','hour','ORIGIN','VIA_1','VIA_2','DESTINATION','move_pop'])\
    #     .sort(['date','hour','ORIGIN','VIA_1','VIA_2','DESTINATION'])\
    #     .groupBy(['date','hour','ORIGIN'])\
    #     .sum()\
    #     .join(df_visitor_count.select(['date','hour','ORIGIN','visitor_number']),on=['date','hour','ORIGIN'],how='inner')\
    #     .display()



# COMMAND ----------


