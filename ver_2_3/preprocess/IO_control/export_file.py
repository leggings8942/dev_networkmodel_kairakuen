import os
from pyspark.sql import DataFrame


def write_parquet_date_file(OUTPUT_PATH:str, date:str, ps_df:DataFrame) -> None:
    # HOUR_UNIT   = 'daily/' or 'hourly/' or 'by30min/' or 'by10min/' or 'by1min/'
    # OUTPUT_PATH = WORK_PATH + PROJECT_NAME + '/preprocess/' + HOUR_UNIT
    
    # 出力パス
    path = OUTPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    # パス先がなければディレクトリの作成
    if not os.path.isdir(path): 
        os.makedirs(path,exist_ok=True)
    # 出力
    ps_df\
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .option('compression', 'snappy')\
        .parquet(path)
    
