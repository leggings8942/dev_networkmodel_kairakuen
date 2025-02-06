# Databricks notebook source
# MAGIC %md
# MAGIC #### ver1.0からver2.0への変更内容
# MAGIC - ⭕️時系列データ作成処理（前処理）とNWM処理部分を分離する
# MAGIC - ⭕️各々が日次更新処理されるように変更する
# MAGIC - ⭕️来訪者数推定処理とNWMの処理に用いる設定ファイルを分離する

# COMMAND ----------

# インポート
import os
import shutil
import time
import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

pd.options.mode.chained_assignment = None

import sys

sys.setrecursionlimit(2000)

# COMMAND ----------

# 設定ファイルパスとファイル名
PATH = '/dbfs/mnt/adintedataexplorer'
AIB_NWM_LIST = PATH +'_ml-datastore/powerbi/custom_order/NWM/来訪者数推定/input/visitor_list_by_projects.csv'
PROCESS_PATH = '/dbfs/mnt/adintedataexplorer_ml-medallion/dev/'
OUTPUT_PATH = PATH +'_ml-datastore/powerbi/custom_order/NWM/来訪者数推定/output/'

# 使わない
CONF_NWM_LIST ='/dbfs/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/config_visitor_list.csv'


# COMMAND ----------

# MAGIC %run ./exec/exec

# COMMAND ----------

# MAGIC %run ./preprocess/preprocess_for_visitor

# COMMAND ----------

# MAGIC %run ./preprocess/imputation_for_visitor

# COMMAND ----------

# MAGIC %run ./export/export

# COMMAND ----------

def read_config_file(aib_list,conf_list):
    aib_nwm_df  = pd.read_csv(aib_list,index_col=0,dtype={'place_id':str})
    conf_nwm_df = pd.read_csv(conf_list,index_col=0)
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

# 設定内容を取り出す
def get_dict(conf_nwm,aib_nwm):
    customer = list(conf_nwm.index)[0]
    conf_dict = conf_nwm.to_dict()
    aib_dict = aib_nwm.to_dict()
    return customer,conf_dict,aib_dict

# COMMAND ----------

# 格納用のフォルダ名を取り出す
def get_folder_name(aib_dict,customer):
    folder_name = aib_dict['folder_name'][customer]
    area_name = aib_dict['area'][customer]
    return folder_name,area_name

# COMMAND ----------

# 設定名(aib,gps)を取り出す
def get_aib_and_gps(conf_dict,customer):
    aib = conf_dict['aib'][customer]
    gps = conf_dict['gps'][customer]
    return aib,gps

# COMMAND ----------

for i in range(0,91):
    # 日付を設定する(NOTE:date_gpsは使わない)
    # date_aib,date_gps = set_date()
    date_aib,date_gps = set_date_past(i)
    print(i,date_aib)
    # 設定ファイルを読み込む(conf_nwm_dfは使わないかも)
    aib_nwm_df,conf_nwm_df = read_config_file(AIB_NWM_LIST,CONF_NWM_LIST)
    # folder_nam,erea別に処理を行う
    for index,row in aib_nwm_df[['place_id','unit_id','rssi']].iterrows():
        # print(row['place_id'],row['unit_id'],row['rssi'])
        # ノートブック実行
        if not row['place_id'] in ['30240','30241','30242','30243','30563']:
            continue
        preprocess_for_visitor(date_aib,row['place_id'],row['unit_id'],row['rssi'])



# COMMAND ----------



# COMMAND ----------


