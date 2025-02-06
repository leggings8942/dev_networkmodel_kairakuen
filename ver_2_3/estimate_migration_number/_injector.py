from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F

import IO_control.export_file as ex
import IO_control.import_file as im
from UseCases._interface import calc_estimate_migration

# 推定回遊人数計算用インターフェース
class original_EM(calc_estimate_migration):
    def __init__(self,
                 ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'],
                 BASE_PATH:str,
                 SECTION:Literal['daily', 'hourly'],
                 ESTIM_VISIT_NAME:str,
                 ESTIM_VISIT_PATH:str,
                 CAUSALITY_PATH:str,
                 OUTPUT_PATH:str,
                 date:str,
                 ENABLE_GROUP_MODE:bool,
                ) -> None:
        super().__init__()
        self.ANALYSIS_OBJ      = ANALYSIS_OBJ
        self.BASE_PATH         = BASE_PATH
        self.SECTION           = SECTION
        self.ESTIM_VISIT_NAME  = ESTIM_VISIT_NAME
        self.ESTIM_VISIT_PATH  = ESTIM_VISIT_PATH
        self.CAUSALITY_PATH    = CAUSALITY_PATH
        self.OUTPUT_PATH       = OUTPUT_PATH
        self.date              = date
        self.ENABLE_GROUP_MODE = ENABLE_GROUP_MODE
    
    def read_visitor_count(self) -> DataFrame:
        folder_path = self.BASE_PATH + self.ESTIM_VISIT_PATH
        ps_res = im.read_csv_date_file(folder_path, self.date, self.ESTIM_VISIT_NAME)
        
        pick_col = 'unit_id' if self.ANALYSIS_OBJ == 'AI_BEACON' else 'place_id'
        pick_col = pick_col  if not self.ENABLE_GROUP_MODE else 'network_id'
        if self.SECTION == 'daily':
            sel_list = ['date',         'place_id', pick_col, 'visitor_number']
        else:
            sel_list = ['date', 'hour', 'place_id', pick_col, 'visitor_number']
        
        ps_res = ps_res.select(sel_list)\
                    .withColumn('ORIGIN',      col(pick_col))\
                    .withColumn('DESTINATION', col(pick_col))
        return ps_res
    
    def read_causal_quantity(self, target:str) -> DataFrame:
        folder_path = self.BASE_PATH + self.CAUSALITY_PATH
        ps_res = im.read_csv_file(folder_path, self.date, target)
        
        if self.SECTION == 'hourly':
            ps_res = ps_res\
                        .withColumn('hour', F.hour('date'))\
                        .withColumn('date', F.to_date(col('date')))
        
        return ps_res
    
    def preprocess_move_val(self, hierarchy:Literal[2, 3, 4], df_caus:DataFrame, df_c_val:DataFrame) -> tuple[list[str], str, DataFrame, DataFrame]:
        if self.SECTION == 'daily':
            sel_list = ['date',         'place_id', 'DESTINATION', 'C-value']
            key_list = ['date',                     'DESTINATION']
        else:
            sel_list = ['date', 'hour', 'place_id', 'DESTINATION', 'C-value']
            key_list = ['date', 'hour',             'DESTINATION']
        
        if   hierarchy == 2:
            influence = '移動影響量'
        elif hierarchy == 3:
            influence = '移動影響量_VIA_1'
        else:
            influence = '移動影響量_VIA_2'
        
        return key_list, influence, df_caus, df_c_val.select(sel_list)
    
    def preprocess_move_val_total(self, df_move_val:DataFrame) -> tuple[list[str], DataFrame]:
        if self.SECTION == 'daily':
            sel_list   = ['ORIGIN', 'date',         'move_val']
            group_list = ['date',         'ORIGIN']
        else:
            sel_list   = ['ORIGIN', 'date', 'hour', 'move_val']
            group_list = ['date', 'hour', 'ORIGIN']
        
        return group_list, df_move_val.select(sel_list)
    
    def preprocess_move_pop(self, df_move_val:DataFrame, df_visit_cnt:DataFrame, df_move_val_total:DataFrame) -> tuple[list[str], DataFrame, DataFrame, DataFrame]:
        if self.SECTION == 'daily':
            sel_list = ['date',         'ORIGIN', 'visitor_number']
            key_list = ['date',         'ORIGIN']
        else:
            sel_list = ['date', 'hour', 'ORIGIN', 'visitor_number']
            key_list = ['date', 'hour', 'ORIGIN']
        
        return key_list, df_move_val, df_visit_cnt.select(sel_list), df_move_val_total
    
    def write_estimate_migration_number(self, hierarchy:Literal[2, 3, 4], f_name:str, df_move_pop:DataFrame) -> None:
        if   hierarchy == 2:
            tmp_list = []
        elif hierarchy == 3:
            tmp_list = ['VIA_1']
        else:
            tmp_list = ['VIA_1', 'VIA_2']
    
        if self.SECTION == 'daily':
            sel_list  = ['date',         'place_id', 'ORIGIN', *tmp_list, 'DESTINATION', 'move_pop']
            sort_list = ['date',         'place_id', 'ORIGIN', *tmp_list, 'DESTINATION']
        else:
            sel_list  = ['date', 'hour', 'place_id', 'ORIGIN', *tmp_list, 'DESTINATION', 'move_pop']
            sort_list = ['date', 'hour', 'place_id', 'ORIGIN', *tmp_list, 'DESTINATION']
        
        folder_path = self.BASE_PATH + self.OUTPUT_PATH + f_name + '/' + self.SECTION + '/'
        df_move_pop = df_move_pop.select(sel_list).sort(sort_list)
        ex.write_parquet_file(folder_path, self.date, df_move_pop)
    