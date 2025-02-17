import time
from typing import Literal

import pandas as pd

from ._interface import upload_to_file
from ._interface import download_to_file


def calc_migration(df_devided:pd.DataFrame) -> pd.DataFrame:
    # df_deviedに期待する構成
    #  | 'date'    | 'ORIGIN' | 'DESTINATION' | '移動影響量'
    # 0, 2022-10-01, 30943,     30943,          0.0
    # 1, 2022-10-01, 30943,     30984,          0.1013
    # 2, 2022-10-01, 30943,     30985,          0.1043
    # 3, 2022-10-01, 30943,     31177,          0.0
    # ・
    # ・
    # ・
    
    # 移動影響量が0のデータを省く
    df_devided   = df_devided[df_devided['移動影響量'] != 0]
    
    # 経由地1のunitリストと時刻リスト
    v_unit_list  = df_devided['DESTINATION'].drop_duplicates().to_list()
    v_time_list  = df_devided['date'].drop_duplicates().to_list()

    # 回遊計算済みのDataFrameリスト
    migrate_list = [pd.DataFrame(columns=['date', 'ORIGIN', 'VIA_1', 'DESTINATION', '移動影響量_VIA_1'])]
    for v_time in v_time_list:
        tmp_devided = df_devided[  df_devided['date']   == v_time]
        
        for v_unit in v_unit_list:
            # 1) 2階層回遊におけるDESTINATIONのunitを抽出
            df_v1_to_d = tmp_devided[tmp_devided['DESTINATION'] == v_unit].rename(columns={'移動影響量':'移動影響量_VIA_0'})
            df_v1_to_d = df_v1_to_d[['date',  'ORIGIN',      '移動影響量_VIA_0']]
        
            # 2) 1)のunitを出発地とするirfを抽出
            df_v1_to_o = tmp_devided[tmp_devided['ORIGIN']      == v_unit].rename(columns={'ORIGIN':'VIA_1', '移動影響量':'移動影響量_VIA_1'})
            df_v1_to_o = df_v1_to_o[['VIA_1', 'DESTINATION', '移動影響量_VIA_1']]
        
            # 3) 交差結合する
            df_v1 = pd.merge(df_v1_to_d, df_v1_to_o, how='cross')
        
            # 4) 出発地・経由地1のirf と 経由地1・目的地のirf を掛ける
            df_v1['移動影響量_VIA_1'] = df_v1['移動影響量_VIA_0'] * df_v1['移動影響量_VIA_1']
        
            # 5) 列を整える
            df_v1 = df_v1.reindex(columns=['date', 'ORIGIN', 'VIA_1', 'DESTINATION', '移動影響量_VIA_1'])
        
            # 6) リストに登録する
            migrate_list.append(df_v1)

    # 縦結合する
    df_migrate = pd.concat(migrate_list, axis=0, ignore_index=True)
    df_migrate = df_migrate.sort_values(['date', 'ORIGIN', 'VIA_1', 'DESTINATION'], ignore_index=True)
    # df_migrateに期待する構成
    #  | 'date'    | 'ORIGIN' | 'VIA_1' | 'DESTINATION' | '移動影響量_VIA_1'
    # 0, 2022-10-01, 30943,     30984,    30943,          0.0
    # 1, 2022-10-01, 30943,     30984,    30984,          0.01087849
    # 2, 2022-10-01, 30943,     30984,    30985,          0.01087849
    # 3, 2022-10-01, 30943,     30984,    31177,          0.0
    # ・
    # ・
    # ・
    
    return df_migrate


def calc_migration_fourth(df_devided:pd.DataFrame, df_migration:pd.DataFrame) -> pd.DataFrame:
    # df_deviedに期待する構成
    #  | 'date'    | 'ORIGIN' | 'DESTINATION' | '移動影響量'
    # 0, 2022-10-01, 30943,     30943,          0.0
    # 1, 2022-10-01, 30943,     30984,          0.1013
    # 2, 2022-10-01, 30943,     30985,          0.1043
    # 3, 2022-10-01, 30943,     31177,          0.0
    # ・
    # ・
    # ・
    
    # df_migrationに期待する構成
    #  | 'date'    | 'ORIGIN' | 'VIA_1' | 'DESTINATION' | '移動影響量_VIA_1'
    # 0, 2022-10-01, 30943,     30984,    30943,          0.0
    # 1, 2022-10-01, 30943,     30984,    30984,          0.01087849
    # 2, 2022-10-01, 30943,     30984,    30985,          0.01087849
    # 3, 2022-10-01, 30943,     30984,    31177,          0.0
    # ・
    # ・
    # ・
    
    # 移動影響量が0のデータを省く
    df_devided   = df_devided[  df_devided[  '移動影響量']       != 0]
    df_migration = df_migration[df_migration['移動影響量_VIA_1'] != 0]
    
    # 経由地2のunitリストと時刻リスト
    o_unit_list  = df_migration['DESTINATION'].drop_duplicates().to_list()
    o_time_list  = df_migration['date'].drop_duplicates().to_list()
    
    # 回遊計算済みのDataFrameリスト
    migrate_list = [pd.DataFrame(columns=['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION', '移動影響量_VIA_2'])]
    for o_time in o_time_list:
        tmp_migration = df_migration[df_migration['date'] == o_time]
        tmp_devided   = df_devided[  df_devided['date']   == o_time]
        
        for o_unit in o_unit_list:
            # 1) 3階層回遊におけるDESTINATIONのunitを抽出
            df_v2_to_d = tmp_migration[tmp_migration['DESTINATION'] == o_unit]
            df_v2_to_d = df_v2_to_d[['date',  'ORIGIN',      'VIA_1', '移動影響量_VIA_1']]
        
            # 2) 1)のunitを出発地とするirfを抽出
            df_v2_to_o = tmp_devided[  tmp_devided['ORIGIN']        == o_unit].rename(columns={'ORIGIN':'VIA_2', '移動影響量':'移動影響量_VIA_2'})
            df_v2_to_o = df_v2_to_o[['VIA_2', 'DESTINATION',          '移動影響量_VIA_2']]
        
            # 3) 交差結合する
            df_v2 = pd.merge(df_v2_to_d, df_v2_to_o, how='cross')
        
            # 4) 出発地・経由地1のirf と 経由地1・経由地2のirf と 経由地2・目的地のirf を掛ける
            df_v2['移動影響量_VIA_2'] = df_v2['移動影響量_VIA_1'] * df_v2['移動影響量_VIA_2']
        
            # 5) 列を整える
            df_v2 = df_v2.reindex(columns=['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION', '移動影響量_VIA_2'])
        
            # 6) リストに登録する
            migrate_list.append(df_v2)

    
    # 縦結合する
    df_migrate = pd.concat(migrate_list, axis=0, ignore_index=True)
    df_migrate = df_migrate.sort_values(['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION'], ignore_index=True)
    # df_migrateに期待する構成
    #  | 'date'    | 'ORIGIN' | 'VIA_1' | 'VIA_2' | 'DESTINATION' | '移動影響量_VIA_2'
    # 0, 2022-10-01, 30943,     30984,    30985,    30943,          0.0
    # 1, 2022-10-01, 30943,     30984,    30985,    30984,          0.00103951
    # 2, 2022-10-01, 30943,     30984,    30985,    30985,          0.00103951
    # 3, 2022-10-01, 30943,     30984,    30985,    31177,          0.0
    # ・
    # ・
    # ・

    return df_migrate


def continuous_migration(use_dl:download_to_file, use_ul:upload_to_file, t_unit:Literal['daily', 'hourly'], date:str, migrate_num:int) -> None:
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}: 因果量・次数中心データファイル取得 計測開始")
    
    causality, centrality = use_dl.read_csv_file_for_nwm(t_unit, date)
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}: 因果量・次数中心データファイル取得時間 {time_diff}秒")
    
    # 人気エリア計算結果 出力
    tmp_path = 'output/人気エリア/' + t_unit + '/'
    use_ul.write_csv_file_with_date(tmp_path, centrality, date, '人気エリア')
    
    # 番兵
    if migrate_num <= 2:
        return None
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}: 回遊3階層解析 計測開始")
    
    # 回遊の計算を行う 
    df_migration = calc_migration(causality)
    # print(df_migration)
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}: 回遊3階層解析時間 {time_diff}秒")
    
    
    # 回遊計算結果 出力
    tmp_path = 'intermediate/' + t_unit + '/'
    use_ul.write_csv_file(tmp_path, df_migration, date, 'migration_3')
    
    # 番兵
    if migrate_num <= 3:
        return None
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}: 回遊4階層解析 計測開始")
    
    # 4階層目(経由地2つ目)の回遊計算を行う
    df_migration_fourth = calc_migration_fourth(causality, df_migration)
    # print(df_migration_fourth)
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}: 回遊4階層解析時間 {time_diff}秒")
    
    
    # 回遊計算結果 出力
    tmp_path = 'intermediate/' + t_unit + '/'
    use_ul.write_csv_file(tmp_path, df_migration_fourth, date, 'migration_4')


