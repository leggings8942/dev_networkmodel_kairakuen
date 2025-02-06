# Databricks notebook source
# 来訪者数推定
def to_csv_for_visitor_daily(date,place_id,unit_id,rssi,df):
    # place_idをindexにする
    df['place_id']=[place_id for _ in range(len(df))]
    df.set_index('place_id',inplace=True)
    # パス先の指定
    path_base = OUTPUT_PATH+'daily/'
    dir_path = f'rssi={rssi}/place_id={place_id}/unit_id={unit_id}/'
    date_path = f'year={date[0:4]}/month={date[0:7]}/date={date}/'

    path = path_base+dir_path+date_path
    # パス先がなければディレクトリの作成
    if not os.path.isdir(path): 
        os.makedirs(path,exist_ok=True)
    # csv出力
    df.to_csv(path+f'{date}_{unit_id}_daily.csv')


# COMMAND ----------

# トレンド詳細
def to_csv_for_visitor_hourly(date,place_id,unit_id,rssi,df):
    # place_idをindexにする
    df['place_id']=[place_id for _ in range(len(df))]
    df.set_index('place_id',inplace=True)
    # date列を設ける
    df['date'] = df['datetime'].apply(lambda x:x[:10])
    df = df[['unit_id','date','datetime','count']]
    # パス先の指定
    path_base = OUTPUT_PATH+'hourly/'
    dir_path = f'rssi={rssi}/place_id={place_id}/unit_id={unit_id}/'
    date_path = f'year={date[0:4]}/month={date[0:7]}/date={date}/'

    path = path_base+dir_path+date_path
    # パス先がなければディレクトリの作成
    if not os.path.isdir(path): 
        os.makedirs(path,exist_ok=True)
    # csv出力
    df.to_csv(path+f'{date}_{unit_id}_hourly.csv')


# COMMAND ----------

# 来訪者数推定(gps)
def to_csv_for_visitor_daily_gps(date,place_id,df):
    # place_idをindexにする
    df.set_index('place_id',inplace=True)
    # パス先の指定
    path_base = OUTPUT_PATH+'daily/'
    dir_path = f'gps/place_id={place_id}/'
    date_path = f'year={date[0:4]}/month={date[0:7]}/date={date}/'

    path = path_base+dir_path+date_path
    # パス先がなければディレクトリの作成
    if not os.path.isdir(path): 
        os.makedirs(path,exist_ok=True)
    # csv出力
    df.to_csv(path+f'{date}_{place_id}_daily.csv')


# COMMAND ----------

# トレンド詳細(gps)
def to_csv_for_visitor_hourly_gps(date,place_id,df):
    # place_idをindexにする
    df.set_index('place_id',inplace=True)
    # date列を設けて整列させる
    df['date'] = df['datetime'].apply(lambda x:x[:10])
    df = df[['date','datetime','count']]
    # パス先の指定
    path_base = OUTPUT_PATH+'hourly/'
    dir_path = f'gps/place_id={place_id}/'
    date_path = f'year={date[0:4]}/month={date[0:7]}/date={date}/'

    path = path_base+dir_path+date_path
    # パス先がなければディレクトリの作成
    if not os.path.isdir(path): 
        os.makedirs(path,exist_ok=True)
    # csv出力
    df.to_csv(path+f'{date}_{place_id}_hourly.csv')

