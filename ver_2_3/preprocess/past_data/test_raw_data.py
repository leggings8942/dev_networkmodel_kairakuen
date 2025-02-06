# Databricks notebook source
from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when

# COMMAND ----------

rssi = 70
path = f'dbfs:/mnt/adintedataexplorer_ml-medallion/dev/silver/time_series_data/daily/rssi={rssi}/'
filename = path+f'year=*/month=*/date=*/*.parquet'

df = (spark.read\
                .option('inferSchema', 'False')
                .option('header', 'True')
                .parquet(filename))\
                .filter(col('unit_id')=='10029922')

df.sort('date').display()

# COMMAND ----------


