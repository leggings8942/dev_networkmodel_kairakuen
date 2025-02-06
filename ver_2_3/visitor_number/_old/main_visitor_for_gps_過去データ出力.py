# Databricks notebook source
# MAGIC %md
# MAGIC #### 来訪者数推定(gps)
# MAGIC - いったん、前週日曜日までを週次更新する、とする

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
PLACE_LIST = PATH +'_ml-datastore/powerbi/custom_order/NWM/来訪者数推定/input/visitor_list_by_projects_gps.csv'
PROCESS_PATH = '/dbfs/mnt/adintedataexplorer_ml-medallion/dev/'
OUTPUT_PATH = PATH +'_ml-datastore/powerbi/custom_order/NWM/来訪者数推定/output/'

# 使わない
CONF_NWM_LIST ='/dbfs/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/config_visitor_list.csv'


# COMMAND ----------

# MAGIC %run ./exec/exec

# COMMAND ----------

# MAGIC %run ./preprocess/preprocess_for_visitor_gps

# COMMAND ----------

# MAGIC %run ./preprocess/imputation_for_visitor

# COMMAND ----------

# MAGIC %run ./export/export

# COMMAND ----------

def read_config_file(place_list,conf_list):
    df_place  = pd.read_csv(place_list,index_col=0,dtype={'place_id':str})
    conf_nwm_df = pd.read_csv(conf_list,index_col=0)
    return df_place,conf_nwm_df

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

for i in range(613,615):
    # 日付を設定する(NOTE:date_aibは使わない)
    # date_aib,date_gps = set_date()
    date_aib,date_gps = set_date_past(i)
    print(i,date_gps)


# COMMAND ----------

for i in range(184,614):
    # 日付を設定する(NOTE:date_aibは使わない)
    # date_aib,date_gps = set_date()
    date_aib,date_gps = set_date_past(i)
    print(i,date_gps)
    # 設定ファイルを読み込む(conf_nwm_dfは使わないかも)
    df_place,conf_nwm_df = read_config_file(PLACE_LIST,CONF_NWM_LIST)
    # print(df_place)
    # print(conf_nwm_df)
    # folder_nam,area別に処理を行う
    for index,row in df_place[['place_id']].iterrows():
        # print(row['place_id'])
        preprocess_for_visitor_gps(date_gps,row['place_id'])



# COMMAND ----------



# COMMAND ----------


