# Databricks notebook source
# # インポート
# import os
# import shutil
# import time
# import datetime

# import numpy as np
# import pandas as pd

# import traceback
# import networkx as nx
# from statsmodels.tsa.api import VAR

# import pyspark.sql.functions as F
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# pd.options.mode.chained_assignment = None

# COMMAND ----------

def get_raw_data_for_aib(date,unit_list):
    # 生データ取得先パス
    aib_table = 'adinte.aibeacon_wifi_log'
    # Sparkデータフレーム取得
    df = spark.table(aib_table).filter(F.col('unit_id').isin(unit_list))\
                                .filter(F.col('date')==date)\
                                .filter(F.col('randomized') == '1')\
                                .filter((70>=F.col('rssi')))\
                                .select(['unit_id','aibeaconid','date','datetime'])\
                                .orderBy(['unit_id','date','datetime'])
    return df

def get_raw_data_for_gps(date,place_list):
    # 生データ取得先パス
    gps_table = 'adinte_analyzed_data.gps_contact'
    # Sparkデータフレーム取得
    df = spark.table(gps_table).filter(F.col('place').isin(place_list))\
                                .filter(F.col('date')==date)\
                                .select(['place_id','adid','date','datetime'])\
                                .orderBy(['place_id','date','datetime'])
    return df

# COMMAND ----------

def rename_from_aib_to_variable(df):
    return df.withColumnRenamed('unit_id','variable')

def rename_from_gps_to_variable(df):
    return df.withColumnRenamed('place_id','variable')

# COMMAND ----------

# UDF
@udf(returnType=StringType())
def separate_hh(datetime):
    return datetime[11:13]
@udf(returnType=StringType())
def separate_mm(datetime):
    return datetime[14:16]+':00'

# COMMAND ----------

def aggregate_one_minute(df):
    # 1分間単位でのユニット計測量
    df_agg = df.withColumn('hh', separate_hh(F.col('datetime')))\
                .withColumn('mm', separate_mm(F.col('datetime')))\
                .withColumn('time', F.concat_ws( ":", "hh", "mm"))\
                .groupBy(['variable','date','time'])\
                .count()\
                .orderBy(['variable','date','time'])\
                .select(['date','time','variable','count'])\
                .withColumn('datetime', F.concat_ws(' ','date','time'))\
                .select(['datetime','variable','count'])\
                .toPandas()
    return df_agg

# COMMAND ----------

def make_base_row(date):
    next_date = datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(days=1)
    base = pd.DataFrame({
        'datetime':[d.strftime('%Y-%m-%d %H:%M:%S') for d in pd.date_range(date,next_date,freq='T')[:-1]]
        })
    # print(base.head(3))
    return base

# COMMAND ----------

# datetime列が[yyyy-mm-dd hh:mm:ss]となっているが計測数が0の行は省略されているので補完する
def imputation_of_data(date,df):
    # 設置場所のリストを作成
    variables = df.drop_duplicates('variable')['variable']
    # ユニット設置場所毎に補完する
    df_output = pd.DataFrame()
    for variable in variables:
        df_val = df.query('variable == @variable')\
                    .merge(make_base_row(date),on='datetime',how='right')\
                    .fillna({'variable':variable,
                            'count':0})\
                    .astype({'count':int})\
                    .set_index('datetime')            
        # print(df_val.head(10))    
        # 縦結合する
        df_output = pd.concat([df_output,df_val],axis=0)
    # print(df_output.head(2))
    return df_output
    

# COMMAND ----------

def create_time_series_data(date,df):
    # 設置場所のリストを作成
    variables = df.drop_duplicates('variable')['variable']
    # [date hour]のindexを作る
    datetime_list = sorted(list(set(list(df.index))))
    # print(datetime_list)
    # 出力用のデータフレームを作成する
    time_series_data = pd.DataFrame(datetime_list,columns=['datetime'])
    for variable in variables:
        df_val = df.query('variable == @variable')\
                .rename(columns={'count':variable})\
                .reset_index(drop=True)
        time_series_data[variable] = df_val[variable]
    # indexをdatetimeにする
    time_series_data = time_series_data.set_index('datetime')
    return time_series_data


# COMMAND ----------

def preprocess_for_aib(date,param_dict):
    unit_list = aib_nwm['unit_id'].to_list()
    # rawデータ取得
    df_count = get_raw_data_for_aib(date,unit_list)
    # unit_idをvariableに名称変更
    df_count = rename_from_aib_to_variable(df_count)
    # 1分間計測データへ再集計
    df_count = aggregate_one_minute(df_count)
    # 未取得時刻の追記
    df_count = imputation_of_data(date,df_count)
    # print(df_count.head(3))
    # 時系列データへ変形
    time_series_data = create_time_series_data(date,df_count)
    # csvファイル作成(export)
    to_csv(time_series_data,'time_series_data',date,param_dict)
    # print(folder_name,area_name)


# COMMAND ----------

def preprocess_for_gps(date,param_dict):
    place_list = param_dict['aib_nwm']['place_id'].to_list()
    # print(place_list)
    # rawデータ取得
    df_count = get_raw_data_for_gps(date,place_list)
    # place_idをvariableに名称変更
    df_count = rename_from_gps_to_variable(df_count)
    # 1分間計測データへ再集計
    df_count = aggregate_one_minute(df_count)
    # 未取得時刻の追記
    df_count = imputation_of_data(date,df_count)
    # print(df_count.head(3))
    # 時系列データへ変形
    time_series_data = create_time_series_data(date,df_count)
    # TODO:係数を掛けて調節する
    # path = '/dbfs/mnt/adintedataexplorer_ml-medalion/dev/registered/info/adid_coef/a2_coef
    
    # csvファイル作成(export)
    to_csv(time_series_data,'time_series_data',date,param_dict)
    # print(folder_name,area_name)


# COMMAND ----------

# # 引数の取り込み
# param_dict = {
#                 'aib'           :aib,
#                 'gps'           :gps,
#                 'date_aib':date_aib,
#                 'date_gps':date_gps,
#                 'aib_nwm':aib_nwm,
#                 'folder_name' :folder_name,
#                 'area_name' :area_name,
#                 }


# COMMAND ----------

def preprocess(param_dict):
    if 1 == param_dict['aib']:
        date = param_dict['date_aib']
        preprocess_for_aib(date,param_dict)
    elif 1 == param_dict['gps']:
        date = param_dict['date_gps']
        preprocess_for_gps(date,param_dict)
    else:
        print('設定がまちがっています')

