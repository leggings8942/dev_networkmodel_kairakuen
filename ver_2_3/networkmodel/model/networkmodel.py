import time
import pandas as pd
from typing import Literal

import pandas as pd

from .causality import calc_nwm
from .continuous_migration import continuous_migration
from ._interface import time_series_model
from ._interface import download_to_file
from ._interface import upload_to_file
from ._interface import UseInterface


def create_time_series_by1min(date:str) -> pd.DataFrame:
    # 開始日時と終了日時を設定
    start_time = f'{date} 06:00:00'
    end_time = f'{date} 18:59:00'
    # 1分単位の時間列を作成
    time_range = pd.date_range(start=start_time, end=end_time, freq='min')
    # DataFrameに変換
    df_time = pd.DataFrame(data=time_range, columns=['datetime'])
    # display(df_time)
    return df_time


def read_by1min(use_dl:download_to_file, date:str, ueid_list:list[str]) -> pd.DataFrame:
    df = use_dl.read_parquet_for_1min(date, ueid_list)
    df['datetime'] = pd.to_datetime(df['minute'], format='%Y-%m-%d %H:%M:%S')
    return df


# 1分間時系列データの取得
# データ構造を[datetime|udid-A|udid-B|...|udid-N|]とする
def get_time_series_data(use_dl:download_to_file, date:str, ueid_list:list[str]) -> pd.DataFrame:
    # 時間計測開始
    time_start = time.perf_counter()
    
    # 1分間時系列データの取得
    df = read_by1min(use_dl, date, ueid_list)
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}:    |- 1分間隔時系列データファイル取得時間 {time_diff}秒")
    
    # データ構造を[datetime|udid-A|udid-B|...|udid-N|]とする
    df_by1min = create_time_series_by1min(date)
    
    for unique_id in ueid_list:
        df_utid = df[df['unique_id'] == unique_id]
        df_utid = df_utid[['datetime', '1min_count']]
        df_utid = df_utid.rename(columns={'1min_count': unique_id})
        # df_utid.display()
        df_by1min = pd.merge(df_by1min, df_utid, on='datetime', how='left')
    
    df_by1min = df_by1min.fillna(0).sort_values(by='datetime', ignore_index=True)
    return df_by1min


def nwm_daily(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, t_unit:Literal['daily'], param_dict:dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    # param_dictに期待する構成
    # dict
    #  |-- date:        str
    #  |-- ueid_list:   list[str]
    #  |-- interval:    str
    
    interval = param_dict['interval']
    date     = param_dict['date']
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}: {interval}-間隔時系列データ準備 計測開始")
    
    # 1分間隔時系列データの取得
    df = get_time_series_data(use_dl, date, param_dict['ueid_list'])
    # print(df.limit(10).display())
    
    # 5分間隔計測に置き換える
    df_5min = df.set_index(keys='datetime').resample(interval).sum()
    # print(df_5min.limit(10).display())
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}: {interval}-間隔時系列データ準備時間 {time_diff}秒")
    
    # 解析対象の時系列データの出力
    tmp_path = 'intermediate/' + t_unit + '/' + param_dict['date'] + '_debug_data/'
    use_ul.write_csv_file(tmp_path, df_5min, param_dict['date'], 'input_time_series_data')
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}: {interval}-間隔時系列データ解析 計測開始")
    
    # 1日単位ごとにNWM計算を行う
    df_debug_inte, df_debug_coef, df_output_causality, df_output_centrality = calc_nwm(calc_model, df_5min)
    # print(df_output_causality.limit(3).display())
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}: {interval}-間隔時系列データ解析時間 {time_diff}秒")
    
    # データフレームの先頭にdateを追記する
    df_output_causality.insert(0,  'date', date)
    df_output_centrality.insert(0, 'date', date)
    # print(df_output_causality.limit(3).display())
    
    # 解析モデルのデバッグ情報の出力
    tmp_path = 'intermediate/' + t_unit + '/' + param_dict['date'] + '_debug_data/'
    use_ul.write_csv_file(tmp_path, df_debug_inte, param_dict['date'], 'var_model_intercept')
    use_ul.write_csv_file(tmp_path, df_debug_coef, param_dict['date'], 'var_model_coefficient')
    
    return df_output_causality, df_output_centrality


def networkmodel(spec_comb:UseInterface, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date:        str
    #  |-- ueid_list:   list[str]
    #  |-- folder_name: str
    #  |-- interval:    str
    
    # NWMを作成する(日別nwm処理)
    t_unit = 'daily'
    print(param_dict['folder_name'], t_unit)
    
    # 時間計測開始
    time_start = time.perf_counter()
    
    causality, centrality = nwm_daily(spec_comb.model, spec_comb.download, spec_comb.upload, t_unit, param_dict)
    # 2ファイルの出力
    spec_comb.upload.write_csv_file_for_nwm(t_unit, param_dict['date'], causality, centrality)
    # 移動影響量の計算
    migrate_num = int(param_dict['migrate_num'])
    if migrate_num > 2:
        continuous_migration(spec_comb.download, spec_comb.upload, t_unit, param_dict['date'], migrate_num)
    
    # 時間計測完了
    time_end   = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"daily 総実行時間: {time_diff}秒")

