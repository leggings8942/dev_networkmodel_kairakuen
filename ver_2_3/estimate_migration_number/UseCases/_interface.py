from abc import ABCMeta, abstractmethod
from typing import Literal
from pyspark.sql import DataFrame

# 推定回遊人数計算用インターフェース
class calc_estimate_migration(metaclass=ABCMeta):
    @abstractmethod
    def read_visitor_count(self) -> DataFrame:
        raise NotImplementedError()
    
    @abstractmethod
    def read_causal_quantity(self, target:str) -> DataFrame:
        raise NotImplementedError()
    
    @abstractmethod
    def preprocess_move_val(self, hierarchy:Literal[2, 3, 4], df_caus:DataFrame, df_c_val:DataFrame) -> tuple[list[str], str, DataFrame, DataFrame]:
        raise NotImplementedError()
    
    @abstractmethod
    def preprocess_move_val_total(self, df_move_val:DataFrame) -> tuple[list[str], DataFrame]:
        raise NotImplementedError()
    
    @abstractmethod
    def preprocess_move_pop(self, df_move_val:DataFrame, df_visit_cnt:DataFrame, df_move_val_total:DataFrame) -> tuple[list[str], DataFrame, DataFrame, DataFrame]:
        raise NotImplementedError()
    
    @abstractmethod
    def write_estimate_migration_number(self, hierarchy:Literal[2, 3, 4], f_name:str, df_move_pop:DataFrame) -> None:
        raise NotImplementedError()