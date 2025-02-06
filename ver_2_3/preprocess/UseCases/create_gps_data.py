from pyspark.sql import DataFrame, types
from pyspark.sql.functions import col

from .create_time_series_data import create_time_series_data_daily
from .create_time_series_data import create_time_series_data_hourly
from .create_time_series_data import create_time_series_data_1min
from ._interface import upload_to_file

def create_data_daily(spec_comb:upload_to_file, df:DataFrame, enable_group_mode:bool) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- adid:        string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    
    print('Creating time series data for daily')
    
    if not enable_group_mode:
        # グループ化しない場合には place id を使用する
        unique_id = 'place_id'
    else:
        # グループ化する場合には network id を使用する
        unique_id = 'network_id'
    
    # DataFrameを整える
    df_count = df\
                .select(['date', 'datetime', unique_id, 'adid'])\
                .withColumnRenamed(unique_id, 'unique_id')\
                .withColumnRenamed('adid',     'unique_id2')

    df_daily = create_time_series_data_daily(df_count)
    df_count = df_daily\
                    .withColumnRenamed('unique_id', unique_id)\
                    .select( ['date', unique_id, 'daily_count'])\
                    .orderBy(['date', unique_id])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',        col('date').cast(       types.StringType()))\
                    .withColumn(unique_id,     col(unique_id).cast(    types.StringType()))\
                    .withColumn('daily_count', col('daily_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('daily/', df_count)

# COMMAND ----------

def create_data_hourly(spec_comb:upload_to_file, df:DataFrame, enable_group_mode:bool) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- adid:        string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    
    print('Creating time series data for hourly')
    
    if not enable_group_mode:
        # グループ化しない場合には place id を使用する
        unique_id = 'place_id'
    else:
        # グループ化する場合には network id を使用する
        unique_id = 'network_id'
    
    # DataFrameを整える
    df_count = df\
            .select(['date', 'datetime', unique_id, 'adid'])\
            .withColumnRenamed(unique_id, 'unique_id')\
            .withColumnRenamed('adid',     'unique_id2')
    
    df_hourly = create_time_series_data_hourly(df_count)
    df_count  = df_hourly\
                    .withColumnRenamed('unique_id', unique_id)\
                    .select( ['date', 'hour', unique_id, 'hourly_count'])\
                    .orderBy(['date', 'hour', unique_id])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',         col('date').cast(        types.StringType()))\
                    .withColumn('hour',         col('hour').cast(        types.StringType()))\
                    .withColumn(unique_id,      col(unique_id).cast(     types.StringType()))\
                    .withColumn('hourly_count', col('hourly_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('hourly/', df_count)

# COMMAND ----------

def create_data_1min(spec_comb:upload_to_file, df:DataFrame, enable_group_mode:bool) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- adid:        string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    
    print('Creating time series data for 1min')
    
    if not enable_group_mode:
        # グループ化しない場合には place id を使用する
        unique_id = 'place_id'
    else:
        # グループ化する場合には network id を使用する
        unique_id = 'network_id'
    
    # DataFrameを整える
    df_count = df\
            .select(['date', 'datetime', unique_id, 'adid'])\
            .withColumnRenamed(unique_id, 'unique_id')\
            .withColumnRenamed('adid',     'unique_id2')
    
    df_1min  = create_time_series_data_1min(df_count)    
    df_count = df_1min\
                    .withColumnRenamed('unique_id', unique_id)\
                    .select( ['date', 'minute', unique_id, '1min_count'])\
                    .orderBy(['date', 'minute', unique_id])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',       col('date').cast(      types.StringType()))\
                    .withColumn('minute',     col('minute').cast(    types.StringType()))\
                    .withColumn(unique_id,    col(unique_id).cast(   types.StringType()))\
                    .withColumn('1min_count', col('1min_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('by1min/', df_count)

def create_gps_data(spec_comb:upload_to_file, df:DataFrame, enable_group_mode:bool) -> None:
    create_data_daily( spec_comb, df, enable_group_mode) # 1日単位でデータを集計し保存する
    create_data_hourly(spec_comb, df, enable_group_mode) # 1時間単位でデータを集計し保存する
    create_data_1min(  spec_comb, df, enable_group_mode) # 1分単位でデータを集計し保存する
