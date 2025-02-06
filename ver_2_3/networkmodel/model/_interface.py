from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
import numpy as np
import pandas as pd
from typing import Literal

#時系列分析用インターフェース
class time_series_model(metaclass=ABCMeta):
    @abstractmethod
    def register(self, data:pd.DataFrame) -> bool:
        raise NotImplementedError()
    
    @abstractmethod
    def fit(self, lags:int, offset:int, solver:str) -> bool:
        raise NotImplementedError()
    
    @abstractmethod
    def select_order(self, maxlag:int, ic:str, solver:str) -> int:
        raise NotImplementedError()
    
    @abstractmethod
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        raise NotImplementedError()
    
    @abstractmethod
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        raise NotImplementedError()

#csvファイル保存用インターフェース
class upload_to_file(metaclass=ABCMeta):
    @abstractmethod
    def write_csv_file(self, path:str, df:pd.DataFrame, date:str, f_name:str) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def write_csv_file_with_date(self, path:str, df:pd.DataFrame, date:str, f_name:str) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def write_csv_file_for_nwm(self, t_unit:Literal['daily', 'hourly'], date:str, causality:pd.DataFrame, centrality:pd.DataFrame) -> None:
        raise NotImplementedError()

#csvファイル取得用インターフェース
class download_to_file(metaclass=ABCMeta):
    @abstractmethod
    def read_csv_file(self, path:str) -> pd.DataFrame:
        raise NotImplementedError()
    
    @abstractmethod
    def read_csv_file_for_nwm(self, t_unit:Literal['daily', 'hourly'], date:str) -> tuple[pd.DataFrame, pd.DataFrame]:
        raise NotImplementedError()
    
    @abstractmethod
    def read_parquet_for_1min(self, date:str, ueid_list:list[str]) -> pd.DataFrame:
        raise NotImplementedError()

# 構造体
@dataclass
class UseInterface:
    model:    time_series_model
    upload:   upload_to_file
    download: download_to_file
