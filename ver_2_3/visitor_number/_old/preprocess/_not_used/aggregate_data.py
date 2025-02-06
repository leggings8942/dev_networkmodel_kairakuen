# Databricks notebook source
# UDF
@udf(returnType=StringType())
def separate_hh(datetime):
    return datetime[11:13]
@udf(returnType=StringType())
def separate_mm(datetime):
    return datetime[14:16]+':00'

# COMMAND ----------

def aggregate_one_minute(df):
    # 1分間単位でのユニット計測量
    df_agg = df.withColumn('hh', separate_hh(F.col('datetime')))\
                .withColumn('mm', separate_mm(F.col('datetime')))\
                .withColumn('time', F.concat_ws( ":", "hh", "mm"))\
                .groupBy(['variable','date','time'])\
                .count()\
                .orderBy(['variable','date','time'])\
                .select(['date','time','variable','count'])\
                .withColumn('datetime', F.concat_ws(' ','date','time'))\
                .select(['datetime','variable','count'])\
                .toPandas()
    return df_agg

# COMMAND ----------

# def aggregate_one_minute_for_gps(df):
#     # 1分間単位でのユニット計測量
#     df_agg = df.withColumn('hh', separate_hh(F.col('datetime')))\
#                 .withColumn('mm', separate_mm(F.col('datetime')))\
#                 .withColumn('time', F.concat_ws( ":", "hh", "mm"))\
#                 .groupBy(['variable','date','time'])\
#                 .count()\
#                 .orderBy(['variable','date','time'])\
#                 .select(['date','time','variable','count'])\
#                 .withColumn('datetime', F.concat_ws(' ','date','time'))\
#                 .select(['datetime','variable','count'])\
#                 .toPandas()
#     return df_agg
