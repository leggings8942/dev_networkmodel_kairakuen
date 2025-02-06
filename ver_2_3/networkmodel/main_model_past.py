# Databricks notebook source
# MAGIC %md
# MAGIC #### ver2.0からver2.1への変更内容
# MAGIC - 対象案件に関係なく、日別と1時間別を出力しておく
# MAGIC - 直交化インパルス応答関数（回遊2階層）、直交化撹乱項をsilverレイヤへ出力する
# MAGIC - 回遊3階層・回遊4階層をsilverレイヤへ出力する
# MAGIC - 人気エリアはgoldレイヤへ出力するので後回しにする

# COMMAND ----------

# インポート
import os
import shutil
import time
import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd

import traceback
import networkx as nx

import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col,lit,when,expr,count,concat
from pyspark.sql.functions import to_timestamp

pd.options.mode.chained_assignment = None

import sys

sys.setrecursionlimit(2000)

# COMMAND ----------

# 設定ファイルパスとファイル名((TODO:configリストのパス先は要検討))
PATH = '/dbfs/mnt/adintedataexplorer'
AIB_NWM_LIST  = PATH + '_ml-datastore/powerbi/nwm/config/network_list_by_projects.csv'
CONF_NWM_LIST = PATH + '_ml-datastore/powerbi/nwm/config/config_network_list.csv'
PROCESS_PATH  = PATH + '_ml-medallion/dev/'
OUTPUT_PATH   = PATH + '_ml-medallion/dev/silver/'


# COMMAND ----------

# MAGIC %run ./model/networkmodel

# COMMAND ----------

# MAGIC %run ./model/causality

# COMMAND ----------

# MAGIC %run ./model/continuous_migration

# COMMAND ----------

# MAGIC %run ./export/export

# COMMAND ----------

def set_date(i):
    dt = datetime.datetime.now()
    # dt = datetime.date(2024,9,2)
    dt_aib = dt + datetime.timedelta(hours=9) - relativedelta(days=1+i)
    dt_gps = dt + datetime.timedelta(hours=9) - relativedelta(days=8+i)
    return dt_aib.strftime('%Y-%m-%d'),dt_gps.strftime('%Y-%m-%d')

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

def main(i):
    # 日付を設定する
    # date_aib,date_gps = set_date()
    date_aib,date_gps = set_date(i)
    print(date_aib,date_gps)
    # 設定ファイルを読み込む
    aib_nwm_df,conf_nwm_df = read_config_file(AIB_NWM_LIST,CONF_NWM_LIST)
    # print(aib_nwm_df)
    # folder_name,area別に処理を行う
    for index,row in conf_nwm_df[['folder_name','area']].iterrows():
        # print(row)
        # 2つの設定ファイルをfolder_nameで抽出する
        aib_nwm,conf_nwm = filter_folder_name(\
            aib_nwm_df,conf_nwm_df,row['folder_name'],row['area'])
        
        # print(aib_nwm.head(3))
        # 対象案件におけるunit_idのリストを取得する
        udid_list = aib_nwm['unit_id'].to_list()
        # print(udid_list)
        param_dict_nwm = {
                        'date_aib':date_aib,
                        'udid_list' :udid_list,
                        'folder_name':row['folder_name'],
                        'area_name'  :row['area'],
                        }
        if not row['folder_name'] == 'nomura_fudosan':
            continue
        # networkmodel処理
        networkmodel(param_dict_nwm)


# COMMAND ----------

# main()
for i in range(0,40):
    main(i)

# COMMAND ----------


