# Databricks notebook source
def create_day_list(start,end):
    return [d.strftime('%Y-%m-%d') for d in pd.date_range(start,end)]

# COMMAND ----------

# UDF
@udf(returnType=StringType())
def separate_hh(datetime):
    return datetime[11:13]
@udf(returnType=StringType())
def separate_mm(datetime):
    return datetime[14:16]+':00'

# COMMAND ----------

def agg_data_for_day(date,place_id):
    # 生データ取得先パス
    gps_table = 'adinte_analyzed_data.gps_contact'
    # Sparkデータフレーム取得
    df = spark.table(gps_table).filter(F.col('place')==place_id)\
                                .filter(F.col('date')==date)\
                                .select(['place_id','adid','date'])\
                                .dropDuplicates()\
                                .groupBy(['place_id','date',])\
                                .count()\
                                .orderBy(['place_id','date'])

    # dfをPandasにする
    df_count = df.toPandas()
    # 取得数0なら補完する
    if 0 == len(df_count):
        df_count = pd.DataFrame({
            'place_id':[place_id],
            'date':[date],
            'count':[0]
        })
    # countを整数値にする
    df_count['count'] = df_count['count'].astype(int)
    return df_count


# COMMAND ----------

def agg_data_for_hour(date,place_id):
    # 生データ取得先パス
    gps_table = 'adinte_analyzed_data.gps_contact'
    # Sparkデータフレーム取得
    df = spark.table(gps_table).filter(F.col('place')==place_id)\
                                .filter(F.col('date')==date)\
                                .select(['place_id','adid','date','datetime'])\
                                .withColumn('hour', separate_hh(F.col('datetime')))\
                                .dropDuplicates()\
                                .groupBy(['place_id','date','hour'])\
                                .count()\
                                .orderBy(['place_id','date','hour'])

    # 0で埋まっている列を突合させる
    zero_list = pd.DataFrame({
        'hour':[str(h) if h>=10 else f'0{h}' for h in range(0,24)]})
    df_count = pd.merge(df.toPandas(),zero_list,on='hour',how='right')\
        .fillna({'place_id':place_id,
                 'date':date,
                 'count':0})
    # countを整数値にする
    df_count['count'] = df_count['count'].astype(int)
    # datetime(yyyy-mm-dd hh:00:00)にする
    df_count['datetime'] = df_count.apply(lambda x:x['date']+' '+x['hour']+':00:00',axis=1)
    # [unit_id,datetime,count]だけにする
    df_count = df_count[['place_id','datetime','count']]
    # print(zero_list)
    return df_count

# COMMAND ----------

def preprocess_for_visitor_gps(date,place_id):
    # # 来訪者数推定（日別）
    # df_daily = agg_data_for_day(date,place_id)
    # # # 欠損処理を行う(TODO:必要になったら実装する)
    # # # df_daily = imputation_visitor_daily(df_daily,start)
    # # 出力する
    # to_csv_for_visitor_daily_gps(date,place_id,df_daily)
    # # print(df_daily.head(5))

    # 詳細トレンド（1時間別）
    df_hourly = agg_data_for_hour(date,place_id)
    # # # 欠損処理を行う
    # # df_hourly = imputation_visitor_hourly(df_hourly,start)
    # # 出力する
    to_csv_for_visitor_hourly_gps(date,place_id,df_hourly)
    # print(df_hourly.head(5))
