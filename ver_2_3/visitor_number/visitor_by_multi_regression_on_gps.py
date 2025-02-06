# Databricks notebook source
WORK_PATH         = dbutils.widgets.get("WORK_PATH")
GPS_DATA_PATH     = dbutils.widgets.get("GPS_DATA_PATH")
NETWORK_LIST      = dbutils.widgets.get("NETWORK_LIST")
PROJECT_NAME      = dbutils.widgets.get('PROJECT_NAME')
SPECIFIED_DATE    = dbutils.widgets.get('SPECIFIED_DATE')
ENABLE_GROUP_MODE = dbutils.widgets.get("ENABLE_GROUP_MODE")

# COMMAND ----------
import os
from datetime import datetime, timedelta

from pyspark import StorageLevel
from pyspark.sql.functions import col,when,count
import pyspark.sql.functions as F

# COMMAND ----------

# '_started'と'_committed_'で始まるファイルを書き込まないように設定
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
# '_SUCCESS'で始まるファイルを書き込まないように設定
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
#パーティション数を増やす
spark.conf.set("spark.sql.shuffle.partitions",200)#2000

# COMMAND ----------

# 設定ファイル取得する
# input_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/network_list_by_gps_projects.csv'
# df_config = (spark.read\
#         .option('inferSchema', 'False')
#         .option('header', 'True')
#         .csv(input_path))\
#         .select(['folder_name','user_id',unique_id,'place_caption'])\
#         .filter(F.col('folder_name')=='akashikaikyo_park')
# df_config.display()
# place_id = '30951'

# COMMAND ----------

# # 設定ファイル取得する
# input_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/nwm/config/network_list_by_gps_projects.csv'
# df_config = (spark.read\
#         .option('inferSchema', 'False')
#         .option('header', 'True')
#         .csv(input_path))\
#         .select(['folder_name','user_id',unique_id,'place_caption'])\
#         .filter(F.col('folder_name')=='akashikaikyo_park_neighbor')\
#         .filter(F.col('place_caption')=='明石海峡公園全体')
# df_config.display()

# COMMAND ----------

# 日付を設定する
if ENABLE_GROUP_MODE.lower() not in ['true', 't']:
    unique_id   = 'place_id'
    required_id = ['place_id']
else:
    unique_id   = 'network_id'
    required_id = ['place_id', 'network_id']

if SPECIFIED_DATE.lower() in ['now', 'n']:
    dt = datetime.now()
    dt = dt - timedelta(days=8) + timedelta(hours=9)
else:
    dt = datetime.strptime(SPECIFIED_DATE, '%Y-%m-%d')

date,date_yymmdd = dt.strftime('%Y-%m-%d'),dt.strftime('%Y%m%d')
print(date)

# COMMAND ----------

PS_PATH     = f'dbfs:{WORK_PATH + PROJECT_NAME}/'
PD_PATH     = f'/dbfs{WORK_PATH + PROJECT_NAME}/'
DATE_PATH   = f'year={date[0:4]}/month={date[0:7]}/date={date}/'
CONFIG_PATH = PS_PATH + NETWORK_LIST

# COMMAND ----------

# 設定ファイル読み込み
config    = (spark.read\
                .option('inferSchema', 'False')
                .option('header', 'True')
                .csv(CONFIG_PATH)\
            )
config    = config\
                .select(['user_id', *required_id, 'apothem'])\
                .dropDuplicates()\
                .orderBy(['user_id', *required_id])
urid_list = sorted(row['user_id']  for row in config.select('user_id' ).distinct().collect())
peid_list = sorted(row['place_id'] for row in config.select('place_id').distinct().collect())
config.display()

# COMMAND ----------

# 生データと設定ファイルとを突合する
gps_table = GPS_DATA_PATH
df_gps_raw_data = spark.table(gps_table)\
                    .filter(col('usr').isin(urid_list))\
                    .filter(col('place').isin(peid_list))\
                    .filter(col('date') == date)
                    # .select([unique_id,'adid','date','datetime'])\
                    # .orderBy([unique_id,'date','datetime'])
df_gps_raw_data = df_gps_raw_data.join(config.select(required_id), on='place_id', how='inner')
df_gps_raw_data.persist(StorageLevel.MEMORY_AND_DISK_DESER)
df_gps_raw_data.display()

# COMMAND ----------

# df_count = df_raw_data.groupBy(unique_id,'date').count().orderBy([unique_id,'date'])
# df_count.display()

# COMMAND ----------

# not unique
df_count_not_uniq = df_gps_raw_data.groupBy(unique_id,'date')\
                    .agg(count('adid').alias('not_uniq_each_count'))\
                    .orderBy([unique_id,'date'])
# df_count_not_uniq.display()

# unique
df_count_uniq = df_gps_raw_data.select([unique_id,'adid','date'])\
                .dropDuplicates()\
                .groupBy(unique_id,'date')\
                .agg(count('adid').alias('uniq_each_count'))\
                .orderBy([unique_id,'date'])
# df_count_uniq.display()
# 突合
df_count = df_count_not_uniq\
            .join(df_count_uniq,on=[unique_id,'date'])\
            .withColumn('avr_send_time_each_unit',F.col('not_uniq_each_count')/F.col('uniq_each_count'))\
            .orderBy([unique_id,'date'])
df_count.display()

# COMMAND ----------



# COMMAND ----------

# 突合
df_test = df_count\
            .join(config, on=unique_id, how='inner')\
            .select(['date', 'user_id', *required_id, 'not_uniq_each_count', 'uniq_each_count', 'avr_send_time_each_unit', 'apothem'])\
            .orderBy('date', 'user_id', *required_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 回帰モデルを実際に当てはめてみる

# COMMAND ----------

# MAGIC %md
# MAGIC ##### adidユニーク数、平均送信回数

# COMMAND ----------

# uniq_each_count             1.568274
# avr_send_time_each_unit   -14.463771
# 明石海峡公園内に対する案件
# 設定ポリゴン半径               col('apothem')   明石海峡公園内
# 学習元における設定ポリゴン半径   180   水族館(オアシスパークにある)
# dtype: float64
df_test_2 = df_test.withColumn('visitor_number',\
    (col('uniq_each_count')*1.568274\
    +col('avr_send_time_each_unit')*(-14.463771)*F.pow(col('apothem')/180, 2))\
    .cast('int'))\
    .withColumn('visitor_number',\
        when(\
            col('visitor_number')>0,col('visitor_number'))\
            .otherwise(col('uniq_each_count')\
            )\
    )\
    .select([ 'user_id', *required_id, 'date', 'visitor_number'])\
    .orderBy(['user_id', *required_id, 'date'])

df_test_2.display()

# COMMAND ----------
OUTPUT_PATH = PD_PATH + 'output/来訪者数推定_OPTMZ4/daily/' + DATE_PATH
output_path = OUTPUT_PATH + f'{date}_optmz4.csv'
df_output_daily = df_test_2.toPandas()

# パス先がなければディレクトリの作成
if not os.path.isdir(OUTPUT_PATH): 
    os.makedirs(OUTPUT_PATH, exist_ok=True)
df_output_daily.set_index('user_id').to_csv(output_path)


# COMMAND ----------

# MAGIC %md
# MAGIC 時間別の出力

# COMMAND ----------

df_gps_raw_data = df_gps_raw_data\
                    .select([unique_id, 'adid', 'date', 'datetime'])\
                    .withColumn('hour', F.hour('datetime'))

# not unique
df_count_not_uniq = df_gps_raw_data.groupBy(unique_id, 'date', 'hour')\
                    .agg(count('adid').alias('not_uniq_each_count'))\
                    .orderBy([unique_id, 'date', 'hour'])
# df_count_not_uniq.display()

# unique
df_count_uniq = df_gps_raw_data.select([unique_id, 'adid', 'date', 'hour'])\
                    .dropDuplicates()\
                    .groupBy(unique_id, 'date', 'hour')\
                    .agg(count('adid').alias('uniq_each_count'))\
                    .orderBy([unique_id, 'date', 'hour'])
# df_count_uniq.display()
# 突合
df_count = df_count_not_uniq\
                    .join(df_count_uniq,on=[unique_id, 'date', 'hour'])\
                    .withColumn('avr_send_time_each_unit', F.col('not_uniq_each_count') / F.col('uniq_each_count'))\
                    .orderBy([unique_id, 'date', 'hour'])
df_count.display()

# COMMAND ----------

# 突合
df_test = df_count\
            .join(config, on=unique_id, how='inner')\
            .select(['date', 'hour', 'user_id', *required_id, 'not_uniq_each_count', 'uniq_each_count', 'avr_send_time_each_unit', 'apothem'])\
            .orderBy('date', 'hour', 'user_id', *required_id)

df_test_2 = df_test\
                .withColumn('visitor_number',
                    (
                        col('uniq_each_count')         * 1.568274 +
                        col('avr_send_time_each_unit') * (-14.463771) * F.pow(col('apothem')/180, 2)
                    ).cast('int'))\
                .withColumn('visitor_number',
                    when(col('visitor_number') > 0, col('visitor_number')).otherwise(col('uniq_each_count'))
                )\
                .select([ 'user_id', *required_id, 'date', 'hour', 'visitor_number'])\
                .orderBy(['user_id', *required_id, 'date', 'hour'])

df_test_2.display()

# COMMAND ----------
OUTPUT_PATH = PD_PATH + 'output/来訪者数推定_OPTMZ4/hourly/' + DATE_PATH
output_path = OUTPUT_PATH + f'{date}_optmz4.csv'
df_output_daily = df_test_2.toPandas()

# パス先がなければディレクトリの作成
if not os.path.isdir(OUTPUT_PATH): 
    os.makedirs(OUTPUT_PATH, exist_ok=True)
df_output_daily.set_index('user_id').to_csv(output_path)

