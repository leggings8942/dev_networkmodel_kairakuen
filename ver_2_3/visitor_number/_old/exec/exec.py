# Databricks notebook source
def set_date():
    dt = datetime.datetime.now()
    # dt = datetime.date(2024,2,19)
    dt_aib = dt + datetime.timedelta(hours=9) - relativedelta(days=1)
    dt_gps = dt + datetime.timedelta(hours=9) - relativedelta(days=8)
    return dt_aib.strftime('%Y-%m-%d'),dt_gps.strftime('%Y-%m-%d')

# COMMAND ----------

# 過去データ取得時に使用する
def set_date_past(i):
    dt = datetime.date.today()
    # dt = datetime.date(2024,2,19)
    dt_aib = dt - datetime.timedelta(days=1+i)
    dt_gps = dt - datetime.timedelta(days=8+i)
    return dt_aib.strftime('%Y-%m-%d'),dt_gps.strftime('%Y-%m-%d')

# COMMAND ----------


