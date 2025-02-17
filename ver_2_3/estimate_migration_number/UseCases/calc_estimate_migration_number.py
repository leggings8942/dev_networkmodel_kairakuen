from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F

from ._interface import calc_estimate_migration

def calc_c_val(df:DataFrame) -> DataFrame:
    res = df.withColumn(
                'C-value',
                F.when(col('visitor_number') > 0, 1 / (col('visitor_number') ** 2)).otherwise(F.lit(1))
            )
    return res

def create_move_val(key_list:list[str], influence:str, df_caus:DataFrame, df_c_val:DataFrame) -> DataFrame:
    res = df_caus\
            .join(df_c_val, on=key_list, how='inner')\
            .withColumn('move_val', col(influence) / col('C-value'))
    return res

def create_move_val_total(group_list:list[str], df_move_val:DataFrame) -> DataFrame:
    res = df_move_val\
            .groupBy(group_list)\
            .sum()\
            .withColumn('sum(move_val)', F.when(col('sum(move_val)') == 0, F.lit(1)).otherwise(col('sum(move_val)')))
    return res

def create_move_pop(key_list:list[str], df_move_val:DataFrame, df_visit_cnt:DataFrame, df_move_val_total:DataFrame) -> DataFrame:    
    res = df_move_val\
            .join(df_visit_cnt,      on=key_list, how='inner')\
            .join(df_move_val_total, on=key_list, how='inner')\
            .withColumn('move_pop', col('visitor_number') * (col('move_val') / col('sum(move_val)')))
    return res

def remove_move_pop(df_move_pop:DataFrame) -> DataFrame:
    res = df_move_pop\
            .filter(col('move_pop') >= 1)
    return res
    
def calc_estimate_migration_number(model:calc_estimate_migration, migrate_num:int):
    zip_list = list(zip(['causality', 'migration_3', 'migration_4'], [2, 3, 4]))[0:migrate_num-1]
    for target, hierarchy in zip_list:
        # 1時間単位集計の推定来訪者数を取得(E=MC^2におけるCに相当する部分)
        df_visit_cnt = model.read_visitor_count()
        # df_visit_cnt.display()
    
        # 各値を２乗する(Covid-19におけるRNAに合わせ逆数をとる)
        df_c_val = calc_c_val(df_visit_cnt)
        # df_c_val.display()
    
        # n階層因果量データ読み込み
        df_caus = model.read_causal_quantity(target)
        # df_caus.display()
    
        # 「O→D」因果量を「Dの時系列データにおける各時刻の値の2乗」で割る。(E=因果量, M=移動人数, C=推定人数)
        # →→→時間単位における「O→D」の人数を得る
        preparations = model.preprocess_move_val(hierarchy, df_caus, df_c_val)
        df_move_val = create_move_val(*preparations)
        # df_move_val.display()
    
        # 「move_val」の分母を計算する
        preparations = model.preprocess_move_val_total(df_move_val)
        df_move_val_total = create_move_val_total(*preparations)
        # df_move_val_total.display()
    
        # 「move_val」をORIGIN基準で割合にして「move_pop」にする
        preparations = model.preprocess_move_pop(df_move_val, df_visit_cnt, df_move_val_total)
        df_move_pop  = create_move_pop(*preparations)
        # df_move_pop.display()
        
        # 「move_pop」が1に満たない行の削除
        df_migrate_num = remove_move_pop(df_move_pop)

        # 出力
        model.write_estimate_migration_number(hierarchy, '推定回遊人数' + str(hierarchy) + '階層', df_migrate_num)