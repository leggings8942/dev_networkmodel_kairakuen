from pyspark.sql import SparkSession, DataFrame

def read_csv_file(INPUT_PATH:str, date:str, p_name:str) -> DataFrame:
    # p_name:処理内容(回遊・人気エリアなど)
    # INPUT_PATH = BASE_PATH + WORK_PATH
    
    spark = SparkSession.getActiveSession()
    pd_data = spark.read\
                    .option('inferSchema', 'True')\
                    .option('header', 'True')\
                    .csv(INPUT_PATH + f'{date}_{p_name}.csv')
    return pd_data

def read_csv_date_file(INPUT_PATH:str, date:str, p_name:str) -> DataFrame:
    # p_name:処理内容(回遊・人気エリアなど)
    # INPUT_PATH = BASE_PATH + WORK_PATH
    
    spark = SparkSession.getActiveSession()
    path  = INPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    pd_data = spark.read\
                    .option('inferSchema', 'True')\
                    .option('header', 'True')\
                    .csv(path + f'{date}_{p_name}.csv')
    return pd_data

def read_parquet_date_file(PARQUET_PATH:str, date:str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    path  = PARQUET_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/*.parquet'
    parquet_df = spark.read\
                    .option('inferSchema', 'True')\
                    .option('header', 'True')\
                    .parquet(path)
    return parquet_df