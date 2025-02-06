# Databricks notebook source
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


# COMMAND ----------

def get_raw_data_for_gps(date,place_list):
    # 生データ取得先パス
    gps_table = 'adinte_analyzed_data.gps_contact'
    # Sparkデータフレーム取得
    df = spark.table(gps_table).filter(F.col('place').isin(place_list))\
                                .filter(F.col('date')==date)\
                                .select(['place_id','adid','date','datetime'])\
                                .orderBy(['place_id','date','datetime'])
    return df
