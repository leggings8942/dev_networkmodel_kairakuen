# Databricks notebook source
def create_time_series_data(date,df):
    # 設置場所のリストを作成
    variables = df.drop_duplicates('variable')['variable']
    # [date hour]のindexを作る
    datetime_list = sorted(list(set(list(df.index))))
    # print(datetime_list)
    # 出力用のデータフレームを作成する
    time_series_data = pd.DataFrame(datetime_list,columns={'datetime'})
    for variable in variables:
        df_val = df.query('variable == @variable')\
                .rename(columns={'count':variable})\
                .reset_index(drop=True)
        time_series_data[variable] = df_val[variable]
    # indexをdatetimeにする
    time_series_data = time_series_data.set_index('datetime')
    return time_series_data

