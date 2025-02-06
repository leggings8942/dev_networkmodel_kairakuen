import numpy as np
import pandas as pd
import networkx as nx
import itertools

from ._interface import time_series_model


# 時系列データを大きい順に並べる
def sort_data(df:pd.DataFrame) -> pd.DataFrame:
    mean_sorted_list = df.mean().sort_values(ascending=False).index
    # print(mean_sorted)
    return df[mean_sorted_list]


'''
インパルス応答関数作成時、エラーの場合はCausalityを全部0にして返す関数
'''
NO_CAUSALITY = 0
def input_causality_for_error(aib_list):
  df_causality_error = pd.DataFrame()
  for o in aib_list:
    for d in aib_list:
      df_tmp = pd.DataFrame(
        {'ORIGIN':[o],
         'DESTINATION':[d],
         '移動影響量':[NO_CAUSALITY]}
      )
      df_causality_error = pd.concat([df_causality_error,df_tmp],axis=0)
  return  df_causality_error

'''
インパルス応答関数作成時、エラーの場合はCentralityを全部0にして返す関数
'''
NO_CENTRALITY = 0
def input_centrality_for_error(aib_list):
  df_centrality = pd.DataFrame({
    'unique_id':aib_list,
    '人気エリア':[NO_CENTRALITY for _ in aib_list]
  })
  return df_centrality

'''
Causalityのデータフレームを作成する関数

parameter:
    tuple_dic:{(from unit,to unit):value}
'''
def create_df_causality(tuple_dic:dict) -> pd.DataFrame:
    # [[30713, 30713, 0.0], [30713, 30714, 3.099], ...]
    # [[from, to, irf], ...]というリストを作る
    list_causality = [[pair[0], pair[1], causality] for pair, causality in tuple_dic.items()]
    df_causality   = pd.DataFrame(list_causality, columns=['ORIGIN', 'DESTINATION', '移動影響量'])
    return df_causality

'''
Centralityのデータフレームを作成する関数
'''
def create_df_centrality(graph:nx.DiGraph, aibeacon_list:list) -> pd.DataFrame:
  # {30865:0.8, 30866:1.3, ...}
  # 次数中心性を計算する
  dict_centrality = nx.degree_centrality(graph)
  # [[30865, 0.8], [30866, 1.3], [30867, 0], ...]
  # 次数中心がある場合は値を入れるが無い場合は0を入れる
  ls2d_centrality = [[beacon, round(dict_centrality[beacon], 4) if beacon in dict_centrality else 0] for beacon in aibeacon_list]
  
  df_centrality = pd.DataFrame(ls2d_centrality, columns=['unique_id', '人気エリア'])
  return df_centrality


'''
計算結果を整形する関数
'''
def output_result(lag:int, unit_list:list, result_irf:np.ndarray[np.float64], graph:nx.DiGraph) -> tuple[dict, nx.DiGraph]:
    tuple_dic = {}
    tmp_irf   = result_irf[lag]
    for idx_from, idx_to in itertools.product(range(0, len(unit_list)), repeat=2):
      irf_tmp   = round(tmp_irf[idx_to][idx_from], 4)
      from_unit = unit_list[idx_from]
      to_unit   = unit_list[idx_to]
      
      # 因果量が正かつ自己回帰でない場合に登録する
      # グラフにおける辺の追加。因果量が負で自己回帰するものは辺として含めない。
      if irf_tmp > 0 and not(from_unit == to_unit):
        tuple_dic[from_unit, to_unit] = irf_tmp
        graph.add_edge(from_unit, to_unit, weight=irf_tmp)
      else: # 因果量が負または自己回帰であれば0とする
        tuple_dic[from_unit, to_unit] = 0

    return tuple_dic, graph


'''
インパルス応答関数に使用された学習係数のフレームワークを作成する関数
'''
def output_debug_coef(lag:int, unit_list:list, intercept:np.ndarray[np.float64], coefficient:np.ndarray[np.float64]) -> tuple[pd.DataFrame, pd.DataFrame]:
  temp_list  = []
  temp_list2 = []
  sort_list  = sorted(unit_list)
  for i in range(1, lag + 1):
    temp_list  = temp_list  + list(map(lambda x: f'{x}_lag{i}', unit_list))
    temp_list2 = temp_list2 + list(map(lambda x: f'{x}_lag{i}', sort_list))
  
  # デバッグ用データフレーム作成
  pd_intercept   = pd.DataFrame(intercept.T,   index=unit_list, columns=['intercept'])
  pd_coefficient = pd.DataFrame(coefficient.T, index=unit_list, columns=temp_list)
  
  # 外生性の担保のため簡易的にソートされた部分を、元に戻す
  pd_intercept   = pd_intercept.sort_index()
  pd_coefficient = pd_coefficient.sort_index()[temp_list2]
  
  return pd_intercept, pd_coefficient
  

def calc_nwm(calc_model:time_series_model, df:pd.DataFrame, lag=10, period=1) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # df = df.set_index('datetime').sort_index()
    # 時系列データを大きい順に並べる
    df_sorted = sort_data(df)
    unit_list = list(df_sorted.columns)
    # print(df.head(5))
    # print(df_sorted.head(5))
    
    df_diff = df_sorted.fillna(0).reset_index(drop=True)
    
    # VARモデル作成・グラフ作成
    G = nx.DiGraph()
    calc_model.register(df_diff)

    try:        
        for beacon in unit_list:
            G.add_node(beacon, count=df[beacon].sum())
        
        # VARモデルを最大ラグ10で赤池情報量基準で最適なモデルを出力する
        lags = calc_model.select_order(maxlag=lag, ic='aic')
        
        # 1期先のインパルス応答関数
        result_irf = calc_model.irf(period=period, orth=True, isStdDevShock=False)
        # print(result_irf)
        
    except Exception as e:
        # TODO:エラー対応
        print(f"エラー情報:{e}")
        df_causality  = input_causality_for_error(unit_list) 
        df_centrality = input_centrality_for_error(unit_list)
        
        intercept   = pd.DataFrame([], index=sorted(unit_list), columns=sorted(unit_list))
        coefficient = pd.DataFrame([], index=sorted(unit_list), columns=sorted(unit_list))
        
        return intercept, coefficient, df_causality, df_centrality
    
    # デバッグのためVARモデルの学習係数を取得
    tmp_coef = calc_model.get_coef()
    intercept, coefficient = output_debug_coef(lags, unit_list, *tmp_coef)

    # 計算結果を逐次出力
    tuple_dic, G = output_result(period, unit_list, result_irf, G)
    
    # 因果量のデータフレームを作る
    df_causality = create_df_causality(tuple_dic)
    # print(df_causality)
    
    # 次数中心性のデータフレームを作る
    df_centrality = create_df_centrality(G, unit_list)
    
    return intercept, coefficient, df_causality, df_centrality

