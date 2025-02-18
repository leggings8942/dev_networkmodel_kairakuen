import numpy as np
import pandas as pd
from typing import Literal
from statsmodels.tsa.api import VAR
from statsmodels.tsa.vector_ar.var_model import VARResultsWrapper
import IO_control.export_file as ex
import IO_control.import_file as im
from model._interface import time_series_model
from model._interface import upload_to_file, download_to_file
from my_lib.time_series_model import Vector_Auto_Regressive
from my_lib.time_series_model import Sparse_Vector_Auto_Regressive
from my_lib.time_series_model import Non_Negative_Vector_Auto_Regressive



class public_VAR(time_series_model):
    def __init__(self) -> None:
        super().__init__()
        self.wrap_model:VAR = None
        self.learned_model:VARResultsWrapper = None
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = VAR(data.to_numpy())
        return True
    
    def fit(self, lags:int, offset:int=0, solver:str="ols") -> bool:
        self.learned_model = self.wrap_model.fit(maxlags=lags, method=solver)
        return True
    
    def select_order(self, maxlag:int, ic:str="aic", solver:str="ols") -> int:
        res = self.wrap_model.select_order(maxlags=maxlag)
        lags = res.selected_orders[ic]
        self.learned_model = self.wrap_model.fit(maxlags=lags, method=solver)
        raise lags
    
    def irf(self, period:int, orth:bool=False, isStdDevShock:bool=False) -> np.ndarray[np.float64]:
        IRF = self.learned_model.irf(periods=period)
        
        if orth:
            res = IRF.orth_irfs
        else:
            res = IRF.irfs
        
        if (not isStdDevShock) and orth:
            res = res / np.diag(IRF.P)
        
        return res
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        params      = self.learned_model.params
        intercept   = params[0,  :]
        coefficient = params[1:, :]
        return intercept, coefficient


class original_VAR(time_series_model):
    def __init__(self) -> None:
        super().__init__()
        self.wrap_model:Vector_Auto_Regressive = None
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = Vector_Auto_Regressive(data.to_numpy())
        return True
    
    def fit(self, lags:int=1, offset:int=0, solver:str="normal equations") -> bool:
        return self.wrap_model.fit(lags=lags, offset=offset, solver=solver)
    
    def select_order(self, maxlag:int=15, ic:str="aic", solver:str="normal equations") -> int:
        return self.wrap_model.select_order(maxlag=maxlag, ic=ic, solver=solver, isVisible=True)
    
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        return self.wrap_model.irf(period=period, orth=orth, isStdDevShock=isStdDevShock)
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.wrap_model.get_coefficient()


class original_SVAR(time_series_model):
    def __init__(self, norm_α:float=0.1, l1_ratio:float=0.1, tol:float=1e-6, max_iterate:int=3e5) -> None:
        super().__init__()
        self.wrap_model:Sparse_Vector_Auto_Regressive = None
        self.norm_α      = norm_α
        self.l1_ratio    = l1_ratio
        self.tol         = tol
        self.max_iterate = round(max_iterate)
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = Sparse_Vector_Auto_Regressive(data.to_numpy(), norm_α=self.norm_α, l1_ratio=self.l1_ratio, tol=self.tol, isStandardization=True, max_iterate=self.max_iterate)
        return True
    
    def fit(self, lags:int=1, offset:int=0, solver:str="FISTA") -> bool:
        return self.wrap_model.fit(lags=lags, offset=offset, solver=solver)
    
    def select_order(self, maxlag:int=15, ic:str="aic", solver:str="FISTA") -> int:
        return self.wrap_model.select_order(maxlag=maxlag, ic=ic, solver=solver, isVisible=True)
    
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        return self.wrap_model.irf(period=period, orth=orth, isStdDevShock=isStdDevShock)
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.wrap_model.get_coefficient()

class original_NNVAR(time_series_model):
    def __init__(self, tol:float=1e-8, max_iterate:int=3e5) -> None:
        super().__init__()
        self.wrap_model:Non_Negative_Vector_Auto_Regressive = None
        self.tol         = tol
        self.max_iterate = round(max_iterate)
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = Non_Negative_Vector_Auto_Regressive(data.to_numpy(), tol=self.tol, isStandardization=True, max_iterate=self.max_iterate)
        return True
    
    def fit(self, lags:int=1, offset:int=0, solver:str="Optimizer Rafael") -> bool:
        return self.wrap_model.fit(lags=lags, offset=offset, solver=solver)
    
    def select_order(self, maxlag:int=15, ic:str="aic", solver:str="Optimizer Rafael") -> int:
        return self.wrap_model.select_order(maxlag=maxlag, ic=ic, solver=solver, isVisible=True)
    
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        return self.wrap_model.irf(period=period, orth=orth, isStdDevShock=isStdDevShock)
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.wrap_model.get_coefficient()

class original_UL(upload_to_file):
    def __init__(self, ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'], OUTPUT_PATH:str) -> None:
        super().__init__()
        self.ANALYSIS_OBJ = ANALYSIS_OBJ
        self.OUTPUT_PATH  = OUTPUT_PATH + '/'
    
    def write_csv_file(self, path:str, df:pd.DataFrame, date:str, f_name:str) -> None:
        return ex.write_csv_file(self.OUTPUT_PATH + path, df, date, f_name)
    
    def write_csv_file_with_date(self, path:str, df:pd.DataFrame, date:str, f_name:str) -> None:
        return ex.write_csv_file_with_date(self.OUTPUT_PATH + path, df, date, f_name)
    
    def write_csv_file_for_nwm(self, t_unit:str, date:Literal['daily', 'hourly'], causality:pd.DataFrame, centrality:pd.DataFrame) -> None:
        return ex.write_csv_file_for_nwm(self.OUTPUT_PATH, t_unit, date, causality, centrality)


class original_DL(download_to_file):
    def __init__(self, ANALYSIS_OBJ:Literal['AI_BEACON', 'GPS_DATA'], INPUT_PATH:str, BY1MIN_PATH:str, ENABLE_GROUP_MODE:bool) -> None:
        super().__init__()
        self.ANALYSIS_OBJ      = ANALYSIS_OBJ
        self.INPUT_PATH        = INPUT_PATH + '/'
        self.BY1MIN_PATH       = BY1MIN_PATH
        self.ENABLE_GROUP_MODE = ENABLE_GROUP_MODE
    
    def read_csv_file(self, path:str) -> pd.DataFrame:
        return im.read_csv_file(self.INPUT_PATH + path)
    
    def read_csv_file_for_nwm(self, t_unit:Literal['daily', 'hourly'], date:str) -> tuple[pd.DataFrame, pd.DataFrame]:
        return im.read_csv_file_for_nwm(self.INPUT_PATH, t_unit, date)
    
    def read_parquet_for_1min(self, date:str, ueid_list:list[str]) -> pd.DataFrame:
        path = self.INPUT_PATH + self.BY1MIN_PATH
        pd_1min = im.read_parquet_file(path, date)
        
        if self.ANALYSIS_OBJ == 'AI_BEACON':
            if not self.ENABLE_GROUP_MODE:
                # グループ化しない場合には unit id を使用する
                unique_id = 'unit_id'
            else:
                # グループ化する場合には network id を使用する
                unique_id = 'network_id'
            
            schema_by1min = {
                'date':        str,
                'user_id':     int,
                'place_id':    int,
                'folder_name': str,
                'minute':      str,
                unique_id:     str,
                '1min_count':  int,
            }
            
            pd_1min = pd_1min.astype(schema_by1min)
            pd_1min = pd_1min[pd_1min[unique_id].isin(ueid_list)]
            pd_1min = pd_1min.rename(columns={unique_id: 'unique_id'})
        else:
            if not self.ENABLE_GROUP_MODE:
                # グループ化しない場合には place id を使用する
                unique_id   = 'place_id'
            else:
                # グループ化する場合には network id を使用する
                unique_id   = 'network_id'
            
            schema_by1min = {
                'date':        str,
                'minute':      str,
                unique_id:     str,
                '1min_count':  int,
            }
            
            pd_1min = pd_1min.astype(schema_by1min)
            pd_1min = pd_1min[pd_1min[unique_id].isin(ueid_list)]
            pd_1min = pd_1min.rename(columns={unique_id: 'unique_id'})
        
        return pd_1min