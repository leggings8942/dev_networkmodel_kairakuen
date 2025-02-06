# Databricks notebook source
def make_base_row(date):
    next_date = datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(days=1)
    base = pd.DataFrame({
        'datetime':[d.strftime('%Y-%m-%d %H:%M:%S') for d in pd.date_range(date,next_date,freq='T')[:-1]]
        })
    # print(base.head(3))
    return base

# COMMAND ----------

# datetime列が[yyyy-mm-dd hh:mm:ss]となっているが計測数が0の行は省略されているので補完する
def imputation_of_data(date,df):
    # 設置場所のリストを作成
    variables = df.drop_duplicates('variable')['variable']
    # ユニット設置場所毎に補完する
    df_output = pd.DataFrame()
    for variable in variables:
        df_val = df.query('variable == @variable')\
                    .merge(make_base_row(date),on='datetime',how='right')\
                    .fillna({'variable':variable,
                            'count':0})\
                    .astype({'count':int})\
                    .set_index('datetime')            
        # print(df_val.head(10))    
        # 縦結合する
        df_output = pd.concat([df_output,df_val],axis=0)
    # print(df_output.head(2))
    return df_output
    
