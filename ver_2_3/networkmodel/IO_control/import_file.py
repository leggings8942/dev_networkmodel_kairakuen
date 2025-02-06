import glob
import pandas as pd
from typing import Literal

def read_csv_file(CSV_PATH:str, schema:dict | None=None) -> pd.DataFrame:
    pd_data = pd.read_csv(CSV_PATH, header=0, dtype=schema)
    return pd_data

def read_parquet_file(INPUT_PATH:str, date:str) -> pd.DataFrame:
    folder_path = INPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/*.parquet'
    file_list   = glob.glob(folder_path)
    if len(file_list) == 0:
        print("folder_path = ", folder_path)
        print("Not Exist.")
        raise ValueError("File could not be found.")
    
    df = pd.read_parquet(file_list[0])
    return df

def read_csv_file_for_nwm(INPUT_PATH:str, t_unit:Literal['daily', 'hourly'], date:str) -> tuple[pd.DataFrame, pd.DataFrame]:
    # INPUT_PATH = BASE_PATH + WORK_PATH + PROJECT_NAME
    
    schema_causality = {
        'date':        str,
        'ORIGIN':      str,
        'DESTINATION': str,
        '移動影響量':    float,
    }
    schema_centrality = {
        'date':      str,
        'unique_id': str,
        '人気エリア':  float,
    }
    
    # 回遊・人気エリアのファイル読み込み
    path = INPUT_PATH + 'intermediate/' + t_unit + '/' + f'{date}_'
    causality  = read_csv_file(path + 'causality.csv',  schema_causality)
    centrality = read_csv_file(path + 'centrality.csv', schema_centrality)
    
    if t_unit == 'daily':
        causality['date']  = pd.to_datetime(causality['date'], format='%Y-%m-%d')
        centrality['date'] = pd.to_datetime(centrality['date'], format='%Y-%m-%d')
    else:
        causality['date']  = pd.to_datetime(causality['date'], format='%Y-%m-%d %H:%M:%S')
        centrality['date'] = pd.to_datetime(centrality['date'], format='%Y-%m-%d %H:%M:%S')
    
    return (causality, centrality)