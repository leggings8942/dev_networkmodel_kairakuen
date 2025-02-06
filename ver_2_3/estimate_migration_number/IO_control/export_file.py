import os
from pyspark.sql import DataFrame

def write_parquet_file(OUTPUT_PATH:str, date:str, df:DataFrame):
    # f_name:プロジェクトフォルダ名
    # o_name:出力先(intermediate or output)
    # t_unit:時間単位(daily or hourly)
    # p_name:処理内容(回遊・人気エリアなど)
    # OUTPUT_PATH = BASE_PATH + WORK_PATH
    
    folder_path = OUTPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    # パス先がなければディレクトリの作成
    if not os.path.isdir(folder_path): 
        os.makedirs(folder_path, exist_ok=True)
    # csv出力
    df.coalesce(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .option('compression', 'snappy')\
        .parquet(folder_path)

