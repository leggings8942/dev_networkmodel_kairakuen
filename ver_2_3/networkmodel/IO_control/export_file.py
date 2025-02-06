import os
import pandas as pd
from typing import Literal


def write_csv_file(OUTPUT_PATH:str, df:pd.DataFrame, date:str, f_name:str) -> None:
    # パス先がなければディレクトリの作成
    if not os.path.isdir(OUTPUT_PATH): 
        os.makedirs(OUTPUT_PATH, exist_ok=True)
    # csv出力
    df.to_csv(OUTPUT_PATH+f'{date}_{f_name}.csv', index=False, header=True)

def write_csv_file_with_date(OUTPUT_PATH:str, df:pd.DataFrame, date:str, f_name:str) -> None:
    folder_path = OUTPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    # パス先がなければディレクトリの作成
    if not os.path.isdir(folder_path): 
        os.makedirs(folder_path, exist_ok=True)
    # csv出力
    df.to_csv(folder_path+f'{date}_{f_name}.csv', index=False, header=True)

def write_csv_file_for_nwm(OUTPUT_PATH:str, t_unit:Literal['daily', 'hourly'], date:str, causality:pd.DataFrame, centrality:pd.DataFrame) -> None:
    # OUTPUT_PATH = BASE_PATH + WORK_PATH + PROJECT_NAME
    # t_unit:時間単位(daily or hourly)

    path = OUTPUT_PATH + f'intermediate/{t_unit}/'
    causality.to_csv( path + f'{date}_causality.csv',  index=False, header=True)
    centrality.to_csv(path + f'{date}_centrality.csv', index=False, header=True)

# # 連続回遊の出力
# def output_for_continuous_migration(t_unit,date,param_dict):
#     folder_name = param_dict['folder_name']
#     area_name   = param_dict['area_name']

#     # 3階層も4階層も出力する
#     for n in range(3,5):
#         # csvファイルの取得
#         path_base = PROCESS_PATH
#         f_path = f'migration_{n}/{t_unit}/{folder_name}/{area_name}/'
#         path = path_base+f_path
#         # aibかgpsかで場合分け(NOTE:aibとgpsで取得日が異なるので別処理になっている)
#         if 1 == param_dict['aib']:
#             index = param_dict['aib_nwm']['place_id'].values[0]
#             df = pd.read_csv(path + f'{date}_migration_{n}.csv',index_col=0)
#             # インデックスを設定する
#             df['place_id'] = [index for _ in range(len(df))]
#             df.set_index('place_id',inplace=True)
#         elif 1 == param_dict['gps']:
#             index = param_dict['aib_nwm']['user_id'].values[0]
#             df = pd.read_csv(path + f'{date}_migration_{n}.csv',index_col=0)
#             # インデックスを設定する
#             df['user_id'] = [index for _ in range(len(df))]
#             df.set_index('user_id',inplace=True)
#         else:
#             print('設定がまちがっています')
#         # 成果物格納フォルダに移行する
#         to_csv_for_continuous_migration(df,n,t_unit,folder_name,area_name,date)


# # 来訪者数推定
# def to_csv_for_visitor_daily(rssi,place_id,unit_id,df):
#     # place_idをindexにする
#     df['place_id']=[place_id for _ in range(len(df))]
#     df.set_index('place_id',inplace=True)
#     # パス先の指定
#     path_base = OUTPUT_PATH+'来訪者数推定/period=daily/'
#     f_path = f'rssi={rssi}/place_id={place_id}/unit_id={unit_id}/'
#     path = path_base+f_path
#     # パス先がなければディレクトリの作成
#     if not os.path.isdir(path): 
#         os.makedirs(path,exist_ok=True)
#     # csv出力
#     df.to_csv(path+f'{unit_id}_daily.csv')


# # トレンド詳細
# def to_csv_for_visitor_hourly(rssi,place_id,unit_id,df):
#     # place_idをindexにする
#     df['place_id']=[place_id for _ in range(len(df))]
#     df.set_index('place_id',inplace=True)
#     # date列を設ける
#     df['date'] = df['datetime'].apply(lambda x:x[:10])
#     df = df[['unit_id','date','datetime','count']]
#     # パス先の指定
#     path_base = OUTPUT_PATH+'来訪者数推定/period=hourly/'
#     f_path = f'rssi={rssi}/place_id={place_id}/unit_id={unit_id}/'
#     path = path_base+f_path
#     # パス先がなければディレクトリの作成
#     if not os.path.isdir(path): 
#         os.makedirs(path,exist_ok=True)
#     # csv出力
#     df.to_csv(path+f'{unit_id}_hourly.csv')

