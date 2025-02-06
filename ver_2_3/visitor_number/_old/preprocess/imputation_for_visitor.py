# Databricks notebook source
# 0のところを、1週間前のそれに置き換える
def imputation_visitor_daily(df,start):
    # インデックスを振り直す
    df.reset_index(inplace=True,drop=True)
    # 0を抽出する
    df_zero = df.query('count==0')
    for index,row in df_zero.iterrows():
        # 0の日にちをdatetime型に変換する
        dt = datetime.datetime.strptime(row['date'],'%Y-%m-%d')
        # 設置日をdatetime型に変換する
        dt_start = datetime.datetime.strptime(start,'%Y-%m-%d')
        # 欠損が初日からの4日間ならば対応しない
        diff = dt - dt_start
        if diff.days < 4 :
            continue
        # 4日前のデータを取得 
        dt_4 = dt - datetime.timedelta(days=4)
        date_week_ago = dt_4.strftime('%Y-%m-%d')

        # print(date)
        count = df.query('date==@date_week_ago')['count'].values[0]
        # 行番号をしりたい
        idx = df.index[df['date']==row['date']].tolist()[0]
        # print(idx,':',count)
        df.at[idx,'count'] = int(count*random.uniform(0.9, 1.1))
    # print(df.tail(25))
    return df

def imputation_visitor_hourly(df,start):
    # インデックスを振り直す
    df.reset_index(inplace=True,drop=True)
    # 0を抽出する
    df_zero = df.query('count==0')
    for index,row in df_zero.iterrows():
        # 0の日にちをdatetime型に変換する
        dt = datetime.datetime.strptime(row['datetime'][:10],'%Y-%m-%d')
        # 設置日をdatetime型に変換する
        dt_start = datetime.datetime.strptime(start,'%Y-%m-%d')
        # 欠損が初日からの4日間ならば対応しない
        diff = dt - dt_start
        if diff.days < 4 :
            continue
        # print(row)
        # 1週間前を取り出す(NOTE:西武池袋対応のための暫定版)
        dt_0 = datetime.datetime.strptime(row['datetime'],'%Y-%m-%d %H:%M:%S')
        dt_4 = dt_0 - datetime.timedelta(days=4)
        datetime_week_ago = dt_4.strftime('%Y-%m-%d %H:%M:%S')
        # print(row['datetime'],type(row['datetime']))
        # print(datetime_week_ago,type(datetime_week_ago))
        # count = df.query('datetime == @datetime_week_ago')['count'].values[0]
        count = df[df['datetime'] == datetime_week_ago]['count'].values[0]
        # 行番号をしりたい
        idx = df.index[df['datetime']==row['datetime']].tolist()[0]
        # print(idx,':',count)
        df.at[idx,'count'] = int(count*random.uniform(0.9, 1.1))
    # print(df.tail(25))
    return df


# COMMAND ----------

# 個別で対応する
