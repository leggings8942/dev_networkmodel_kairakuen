import numpy as np
import scipy as sp
import pandas as pd
from scipy import stats
from sklearn.linear_model import ElasticNet

def modified_cholesky(x):
    if type(x) is list:
        x = np.array(x)
        
    if x.ndim != 2:
        print(f"x dims = {x.ndim}")
        raise ValueError("エラー：：次元数が一致しません。")
    
    if x.shape[0] != x.shape[1]:
        print(f"x shape = {x.shape}")
        raise ValueError("エラー：：正方行列ではありません。")
    
    n = x.shape[0]
    d = np.diag(x).copy()
    L = np.tril(x, k=-1).copy() + np.identity(n)
    
    for idx1 in range(1, n):
        prev = idx1 - 1
        tmp  = d[0:prev] if d[0:prev].size != 0 else 0
        tmp  = np.dot(L[idx1:, 0:prev], (L[prev, 0:prev] * tmp).T)
        
        DIV  = d[prev] if d[prev] != 0 else 1e-16
        L[idx1:, prev] = (L[idx1:, prev] - tmp) / DIV
        d[idx1]       -= np.sum((L[idx1, 0:idx1] ** 2) * d[0:idx1])
    
    d = np.diag(d)
    return L, d

def log_likelihood_of_normal_distrubution(x, mean, cov):
    assert x.shape == mean.shape,        f"argument sizes do not match:: x.shape = {x.shape}, mean.shape = {mean.shape}"
    assert cov.shape[0] == cov.shape[1], f"covariance matrix must be square:: cov.shape[0] = {cov.shape[0]}, cov.shape[1] = {cov.shape[1]}"
    assert x.shape[0] == cov.shape[0],   f"number of dimensions of convariance matrix and input vector do not match:: x.shape[0] = {x.shape[0]}, cov.shape[0] = {cov.shape[0]}"
    
    try:
        diff  = x - mean
        sigma = np.dot(np.linalg.pinv(cov), diff)
    except Exception as e:
        try:
            sigma = np.linalg.solve(cov, diff)
        except Exception as e:
            sigma = np.zeros_like(diff)
    finally:
        mult  = np.dot(diff.T, sigma)
        mult  = np.abs(mult)
        mult  = np.diag(mult)
    
    d              = x.shape[0]
    log_likelihood = d * np.log(2 * np.pi) + np.log(np.abs(np.linalg.det(cov)) + 1e-256) + mult
    return -log_likelihood / 2

# 軟判別閾値関数
def soft_threshold(x, α):
    return np.sign(x) * np.maximum(np.abs(x) - α, 0)

def soft_maximum(x, α):
    sign = np.empty_like(x)
    sign[x >= 0] =  1
    sign[x <  0] = -1
    return sign * (np.abs(x) + α)

class Update_Rafael:
    def __init__(self, alpha=0.01, beta=0.99, isSHC=False):
        self.alpha   = alpha
        self.beta    = beta
        self.time    = 0
        self.beta_t  = 1
        self.beta_t2 = 1
        self.m = np.array([])
        self.v = np.array([])
        self.w = np.array([])
        self.e = np.array([])
        self.σ_coef = 0
        self.isSHC = isSHC

    def update(self, grads):
        if self.time == 0:
            self.m = np.zeros(grads.shape)
            self.v = np.zeros(grads.shape)
            self.w = np.zeros(grads.shape)
            self.σ_coef = (1 + self.beta) / 2
            
            if self.isSHC:
                self.e = np.zeros(grads.shape)
        
        ε = 1e-32
        self.time   += 1
        self.beta_t *= self.beta

        self.m = self.beta * self.m + (1 - self.beta) * grads
        m_hat = self.m / (1 - self.beta_t)

        self.v = self.beta * self.v + (1 - self.beta) * (grads ** 2)
        self.w = self.beta * self.w + (1 - self.beta) * ((grads / soft_maximum(m_hat, ε) - 1) ** 2)
        
        if self.beta - self.beta_t > 0.1:
            v_hat  = self.v * self.σ_coef / (self.beta - self.beta_t)
            w_hat  = self.w * self.σ_coef / (self.beta - self.beta_t)
            σ_com  = np.sqrt((v_hat + w_hat + ε) / 2)
            
            o_newt = self.alpha * m_hat / σ_com
            
            # self-healing canonicalization
            if self.isSHC:
                self.beta_t2 *= self.beta
                
                self.e  = self.beta * self.e + (1 - self.beta) * o_newt * np.sum(o_newt, axis=0) / 2
                e_hat   = self.e / (1 - self.beta_t2)
                o_erro  = e_hat / σ_com
                
                σ_ab    = np.sum(o_newt * o_erro,   axis=0)
                σ_a2    = np.sum(np.square(o_newt), axis=0)
                σ_b2    = np.sum(np.square(o_erro), axis=0)
                r       = (σ_ab - np.sqrt(σ_a2 + np.square(σ_ab) - σ_a2 * σ_b2)) / (σ_b2 - 1)
                output  = o_newt - o_erro * r
            else:
                output = o_newt
        else:
            output = self.alpha * np.sign(grads)
        
        return output


class Vector_Auto_Regressive:
    def __init__(self, train_data, tol=1e-7, max_iterate=100000, random_state=None) -> None:
        if type(train_data) is pd.core.frame.DataFrame:
            train_data = train_data.to_numpy()
        
        if type(train_data) is list:
            train_data = np.array(train_data)
        
        if type(train_data) is not np.ndarray:
            print(f"type(train_data) = {type(train_data)}")
            print("エラー：：Numpy型である必要があります。")
            raise
        
        if train_data.ndim != 2:
            print(f"train_data dims = {train_data.ndim}")
            print("エラー：：次元数が一致しません。")
            raise
        
        self.train_data          = train_data
        self.lags                = 0
        self.alpha               = np.zeros([1, 1])
        self.alpha0              = np.zeros([1, 1])
        self.sigma               = np.zeros([1, 1])
        self.tol                 = tol
        self.solver              = ""
        self.data_num            = 0
        self.max_iterate         = max_iterate
        self.unbiased_dispersion = 0
        self.dispersion          = 0
        self.ma_inf              = np.zeros([1, 1])
        self.learn_flg           = False

        self.random_state = random_state
        if random_state != None:
            self.random = np.random
            self.random.seed(seed=self.random_state)
        else:
            self.random = np.random
            
    def copy(self):
        buf = []
        buf = buf + [self.train_data.copy()]
        buf = buf + [self.lags]
        buf = buf + [self.alpha.copy()]
        buf = buf + [self.alpha0.copy()]
        buf = buf + [self.sigma.copy()]
        buf = buf + [self.tol]
        buf = buf + [self.solver]
        buf = buf + [self.data_num]
        buf = buf + [self.max_iterate]
        buf = buf + [self.unbiased_dispersion]
        buf = buf + [self.dispersion]
        buf = buf + [self.ma_inf.copy()]
        buf = buf + [self.learn_flg]
        buf = buf + [self.random_state]
        buf = buf + [self.random]
        
        return buf
    
    def restore(self, buf):
        self.train_data          = buf[0]
        self.lags                = buf[1]
        self.alpha               = buf[2]
        self.alpha0              = buf[3]
        self.sigma               = buf[4]
        self.tol                 = buf[5]
        self.solver              = buf[6]
        self.data_num            = buf[7]
        self.max_iterate         = buf[8]
        self.unbiased_dispersion = buf[9]
        self.dispersion          = buf[10]
        self.ma_inf              = buf[11]
        self.learn_flg           = buf[12]
        self.random_state        = buf[13]
        self.random              = buf[14]
        
        return True

    def fit(self, lags=1, offset=0, solver="normal equations") -> bool:
        # caution!!!
        # OLS(Ordinary Learst Squares)推定量を計算する際に
        # 擬似逆行列(pinv関数)を使用している箇所が存在する
        # この処理は逆行列が存在しない場合(行列式が0の場合)に発火する
        # しかし理論的には逆行列が存在しない時系列データの組み合わせは極めて稀である(無視できる)
        # 入力された時系列データ自体にミスが存在する(0の定数列になっている等)可能性が高い
        # statsmodels.tsa.vector_ar.var_model.VARではこのような時系列データを入力として与えた場合には
        # エラーを出力するようになっている
        # 本ライブラリにおいてエラーの出力を行わないのは、近似的にでも処理結果が欲しいためである
        
        tmp_train_data = self.train_data[offset:]
        nobs           = len(tmp_train_data)
        
        if not nobs - lags - tmp_train_data.shape[1] * lags - 1 > 0:
            # データ数に対して、最尤推定対象が多すぎる
            self.learn_flg = False
            return self.learn_flg
        
        x_data         = np.array([tmp_train_data[t-lags : t][::-1].ravel() for t in range(lags, nobs)])
        y_data         = tmp_train_data[lags:]
        
        self.lags = lags
        num, s    = x_data.shape
        if solver == "normal equations":
            #正規方程式
            A = np.hstack([x_data, np.ones([num, 1])])
            b = y_data
            try:
                x = np.dot(np.linalg.pinv(np.dot(A.T, A)), np.dot(A.T, b))
            except np.linalg.LinAlgError as e:
                # x = np.dot(np.linalg.inv( np.dot(A.T, A)), np.dot(A.T, b))
                x = np.linalg.solve(np.dot(A.T, A), np.dot(A.T, b))
                
            self.alpha, self.alpha0 = x[0:s, :], x[s, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
        else:
            raise
        
        # なぜか、共分散行列の計算に特殊な計算方法が採用されている
        # statsmodels.tsa.vector_ar.var_model.VAR を参照のこと
        # _estimate_var関数内にて当該の記述を発見
        # どうやら不偏共分散の推定量らしい
        # math:: \frac{1}{T - Kp - 1} Y^\prime (I_T - Z (Z^\prime Z)^{-1} Z^\prime) Y
        # この式が元になっているらしい
        # よくわからないが、この式を採用することにする
        denominator   = y_data.shape[0] - y_data.shape[1] * lags - 1
        
        self.learn_flg = True
        y_pred         = self.predict(x_data)
        diff           = y_pred - y_data
        self.sigma     = np.dot(diff.T, diff) / denominator
        self.solver    = solver
        self.data_num  = num
        self.unbiased_dispersion = denominator
        self.dispersion          = y_data.shape[0]

        return self.learn_flg

    def predict(self, test_data) -> np.ndarray:
        if type(test_data) is pd.core.frame.DataFrame:
            test_data = test_data.to_numpy()
        
        if type(test_data) is list:
            test_data = np.array(test_data)
        
        if type(test_data) is not np.ndarray:
            print(f"type(test_data) = {type(test_data)}")
            print("エラー：：Numpy型である必要があります。")
            raise
        
        if test_data.ndim != 2:
            print(f"test_data dims = {test_data.ndim}")
            print("エラー：：次元数が一致しません。")
            raise
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        y_pred = np.dot(test_data, self.alpha) + self.alpha0
        
        return y_pred
    
    def get_RSS(self) -> np.ndarray:
        nobs   = len(self.train_data)
        x_data = np.array([self.train_data[t-self.lags : t][::-1].ravel() for t in range(self.lags, nobs)])
        y_data = self.train_data[self.lags:]
        
        y_pred = self.predict(x_data)

        rss = np.square(y_data - y_pred)
        rss = np.sum(rss, axis=0)
        return rss
    
    def get_coefficient(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.alpha0, self.alpha
    
    def log_likelihood(self, offset=0) -> np.float64:
        # なぜか、対数尤度の計算に特殊な計算方法が採用されている
        # statsmodels.tsa.vector_ar.var_model を参照のこと
        # var_loglike関数内にて当該の記述を発見
        # どうやらこれで対数尤度を計算できるらしい
        # math:: -\left(\frac{T}{2}\right) \left(\ln\left|\Omega\right| - K\ln\left(2\pi\right) - K\right)
        # この式が元になっているらしい
        # さっぱり理解できないため、通常通りに計算することにする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        tmp_train_data = self.train_data[offset:]
        nobs           = len(tmp_train_data)
        
        x_data = np.array([tmp_train_data[t-self.lags : t][::-1].ravel() for t in range(self.lags, nobs)])
        y_data = tmp_train_data[self.lags:]
        y_pred = self.predict(x_data)

        # 不偏推定共分散量を通常の推定共分散量に直す
        tmp_sigma      = self.sigma * self.unbiased_dispersion / self.dispersion
        
        log_likelihood = log_likelihood_of_normal_distrubution(y_data.T, y_pred.T, tmp_sigma)
        log_likelihood = np.sum(log_likelihood)

        return log_likelihood
    
    def model_reliability(self, ic="aic", offset=0) -> np.float64:
        # statsmodels.tsa.vector_ar.var_model.VARResults を参照のこと
        # info_criteria関数内にて当該の記述を発見
        # 赤池情報基準やベイズ情報基準をはじめとした情報基準が特殊な形に変形されている
        # これは、サンプル数を考慮した改良版らしい
        # これを採用することとする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        num = self.data_num
        k   = self.alpha.size + self.alpha0.size
        
        # caution!!!
        # 本ライブラリでは、データ数に対して最尤推定対象が多い場合にもできる限り処理を続けるように調整してある
        # しかし、この場合に分散共分散行列の正定値性が保てなくなるという問題が発生する
        # また、入力された時系列データ自体に誤りが存在する場合にも正定値性が保てなくなる
        # 正定値行列でない場合には対数尤度の計算ができなくなる
        # この問題の対策のために対数尤度の近似値を求める処理に変更していることに注意
        # 参考URL:
        # https://seetheworld1992.hatenablog.com/entry/2017/03/22/194932
        
        # 対数尤度の計算
        log_likelihood = -2 * self.log_likelihood(offset=offset)

        inf = 0
        if ic == "aic":
            #inf = -2 * log_likelihood + 2 * k
            inf = log_likelihood / num + 2 * k / num
        elif ic == "bic":
            #inf = -2 * log_likelihood + k * np.log(num)
            inf = log_likelihood / num + k * np.log(num) / num
        elif ic == "hqic":
            #inf = -2 * log_likelihood + 2 * k * np.log(np.log(num))
            inf = log_likelihood / num + 2 * k * np.log(np.log(num)) / num
        else:
            raise

        return inf

    def select_order(self, maxlag=15, ic="aic", solver="normal equations", isVisible=False) -> int:
        if isVisible == True:
            print(f"AR model | {ic}", flush=True)
        
        nobs = len(self.train_data)
        if nobs <= maxlag:
            maxlag = nobs - 1

        model_param = []
        for lag in range(1, maxlag + 1):
            flg = self.fit(lags=lag, offset=maxlag - lag, solver=solver)
            
            if flg:
                rel = self.model_reliability(ic=ic, offset=maxlag - lag)
                model_param.append([rel, lag])
            else:
                rel = np.finfo(np.float64).max
                model_param.append([rel, lag])

            if isVisible == True:
                print(f"AR({lag}) | {rel}", flush=True)
        
        res_rel, res_lag = np.finfo(np.float64).max, 0
        for elem in model_param:
            tmp_rel, tmp_lag = elem
            if res_rel > tmp_rel:
                res_rel = tmp_rel
                res_lag = tmp_lag
        
        res_lag = res_lag if res_lag != 0 else 1
        self.fit(lags=res_lag, offset=0, solver=solver)
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了しませんでした。")
            raise
        
        if isVisible == True:
            print(f"selected orders | {res_lag}", flush=True)

        return self.lags
    
    def test_causality(self, causing=0, caused=1):
        backup = self.copy()
        tmp_train_data = backup[0]
        tmp_lags       = backup[1]
        tmp_alpha      = backup[2]
        tmp_solver     = backup[6]
        tmp_data_num   = backup[7]

        self.fit(lags=tmp_lags, solver=tmp_solver)
        rss1 = self.get_RSS()[caused]

        caused = caused - 1 if causing < caused else caused
        self.train_data = np.delete(tmp_train_data, causing, axis=1)
        self.fit(lags=tmp_lags, solver=tmp_solver)
        rss0 = self.get_RSS()[caused]

        num    = tmp_train_data.shape[1]
        Fvalue = (rss0 - rss1)/num / (rss1 / (tmp_data_num - tmp_alpha.shape[0] - 1))
        pvalue = stats.chi2.sf(x=Fvalue*num, df=num)

        self.restore(backup)

        return Fvalue*num, pvalue
    
    def ma_replace(self, max=10):
        ma_inf = np.zeros([max + 1, self.train_data.shape[1], self.train_data.shape[1]])
        ma_inf[0, :, :] = np.identity(self.train_data.shape[1])
        
        x_data = ma_inf[0, :, :]
        for _ in range(1, self.lags):
            x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])

        for idx in range(1, max + 1):
            ma_inf[idx, :, :] = np.dot(self.alpha.T, x_data)
            x_data = np.vstack([ma_inf[idx, :, :], x_data[:-self.train_data.shape[1], :]])
        
        self.ma_inf = ma_inf
        return self.ma_inf

    def irf(self, period=30, orth=False, isStdDevShock=True):
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise

        if orth == True:
            A, D = modified_cholesky(self.sigma)

            irf = np.zeros([period + 1, self.train_data.shape[1], self.train_data.shape[1]])
            if isStdDevShock:
                irf[0, :, :] = np.dot(A, np.sqrt(D))
            else:
                irf[0, :, :] = np.dot(A, np.identity(self.train_data.shape[1]))

            x_data = irf[0, :, :]
            for _ in range(1, self.lags):
                x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])
            
            for idx in range(1, period + 1):
                #tmp = self.alpha.reshape(self.lags, self.train_data.shape[1], self.train_data.shape[1])
                #tmp = tmp.swapaxes(1,2).reshape(self.lags * self.train_data.shape[1], self.train_data.shape[1])
                #irf[idx, :, :] = np.dot(x_data, tmp)
                irf[idx, :, :] = np.dot(self.alpha.T, x_data)
                x_data = np.vstack([irf[idx, :, :], x_data[:-self.train_data.shape[1], :]])
            """irf_data = self.irf(period, orth=False)
            L = np.linalg.cholesky(self.sigma)
            irf = np.array([np.dot(coefs, L) for coefs in irf_data])"""

        else:
            irf = self.ma_replace(period)

        return irf
    
    def fevd(self, period=30):
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        # 不偏推定共分散量を通常の推定共分散量に直す
        tmp_sigma = self.sigma * self.unbiased_dispersion / self.dispersion
        A, D = modified_cholesky(tmp_sigma)
        
        fevd = np.zeros([period + 1, self.train_data.shape[1], self.train_data.shape[1]])
        fevd[0, :, :] = A
        
        x_data = fevd[0, :, :]
        for _ in range(1, self.lags):
            x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])
        
        fevd[0, :, :] = fevd[0, :, :] ** 2
        for idx in range(1, period + 1):
            fevd[idx, :, :] = np.dot(self.alpha.T, x_data)
            x_data = np.vstack([fevd[idx, :, :], x_data[:-self.train_data.shape[1], :]])
            
            fevd[idx, :, :] = fevd[idx, :, :] ** 2
        
        fevd = fevd.cumsum(axis=0)
        for idx in range(0, period + 1):
            fevd[idx, :, :] = np.dot(fevd[idx, :, :], D)
        
        for idx in range(0, period + 1):
            fevd[idx, :, :] = fevd[idx, :, :] / np.sum(fevd[idx, :, :], axis=1).reshape([self.train_data.shape[1], 1])
        """fevd = self.irf(period=period, orth=True)
        fevd = (fevd ** 2).cumsum(axis=0)
        for idx in range(0, period + 1):
            fevd[idx, :, :] = fevd[idx, :, :] / np.sum(fevd[idx, :, :], axis=1).reshape([self.train_data.shape[1], 1])"""
        
        return fevd


class Sparse_Vector_Auto_Regressive:
    def __init__(self,
                 train_data,                         # 学習対象時系列データ
                 norm_α:float=1.0,                   # L1・L2正則化パラメータの強さ
                 l1_ratio:float=0.1,                 # L1・L2正則化の強さ配分・比率
                 tol:float=1e-6,                     # 許容誤差
                 isStandardization:bool=True,        # 標準化処理の適用有無
                 max_iterate:int=300000,             # 最大ループ回数
                 random_state=None) -> None:         # 乱数のシード値
        if type(train_data) is pd.core.frame.DataFrame:
            train_data = train_data.to_numpy()
        
        if type(train_data) is list:
            train_data = np.array(train_data)
        
        if type(train_data) is not np.ndarray:
            print(f"type(train_data) = {type(train_data)}")
            print("エラー：：Numpy型である必要があります。")
            raise
        
        if train_data.ndim != 2:
            print(f"train_data dims = {train_data.ndim}")
            print("エラー：：次元数が一致しません。")
            raise
        
        self.train_data          = train_data
        self.lags                = 0
        self.alpha               = np.zeros([1, 1])
        self.alpha0              = np.zeros([1, 1])
        self.sigma               = np.zeros([1, 1])
        self.norm_α              = np.abs(norm_α)
        self.l1_ratio            = np.where(l1_ratio < 0, 0, np.where(l1_ratio > 1, 1, l1_ratio))
        self.isStandardization   = isStandardization
        self.x_mean              = np.zeros([1, 1])
        self.x_std_dev           = np.ones([1, 1])
        self.y_mean              = np.zeros([1, 1])
        self.y_std_dev           = np.ones([1, 1])
        self.tol                 = tol
        self.solver              = ""
        self.data_num            = 0
        self.max_iterate         = round(max_iterate)
        self.unbiased_dispersion = 0
        self.dispersion          = 0
        self.ma_inf              = np.zeros([1, 1])
        self.learn_flg           = False

        self.random_state = random_state
        if random_state != None:
            self.random = np.random
            self.random.seed(seed=self.random_state)
        else:
            self.random = np.random
            
    def copy(self):
        buf = []
        buf = buf + [self.train_data.copy()]
        buf = buf + [self.lags]
        buf = buf + [self.alpha.copy()]
        buf = buf + [self.alpha0.copy()]
        buf = buf + [self.sigma.copy()]
        buf = buf + [self.norm_α]
        buf = buf + [self.l1_ratio]
        buf = buf + [self.isStandardization]
        buf = buf + [self.x_mean]
        buf = buf + [self.x_std_dev]
        buf = buf + [self.y_mean]
        buf = buf + [self.y_std_dev]
        buf = buf + [self.tol]
        buf = buf + [self.solver]
        buf = buf + [self.data_num]
        buf = buf + [self.max_iterate]
        buf = buf + [self.unbiased_dispersion]
        buf = buf + [self.dispersion]
        buf = buf + [self.ma_inf.copy()]
        buf = buf + [self.learn_flg]
        buf = buf + [self.random_state]
        buf = buf + [self.random]
        
        return buf
    
    def restore(self, buf):
        self.train_data          = buf[0]
        self.lags                = buf[1]
        self.alpha               = buf[2]
        self.alpha0              = buf[3]
        self.sigma               = buf[4]
        self.norm_α              = buf[5]
        self.l1_ratio            = buf[6]
        self.isStandardization   = buf[7]
        self.x_mean              = buf[8]
        self.x_std_dev           = buf[9]
        self.y_mean              = buf[10]
        self.y_std_dev           = buf[11]
        self.tol                 = buf[12]
        self.solver              = buf[13]
        self.data_num            = buf[14]
        self.max_iterate         = buf[15]
        self.unbiased_dispersion = buf[16]
        self.dispersion          = buf[17]
        self.ma_inf              = buf[18]
        self.learn_flg           = buf[19]
        self.random_state        = buf[20]
        self.random              = buf[21]
        
        return True

    def fit(self, lags:int=1, offset:int=0, solver:str='external library', visible_flg:bool=False) -> bool:
        # caution!!!
        # OLS(Ordinary Learst Squares)推定量を計算する際に
        # 擬似逆行列(pinv関数)を使用している箇所が存在する
        # この処理は逆行列が存在しない場合(行列式が0の場合)に発火する
        # しかし理論的には逆行列が存在しない時系列データの組み合わせは極めて稀である(無視できる)
        # 入力された時系列データ自体にミスが存在する(0の定数列になっている等)可能性が高い
        # statsmodels.tsa.vector_ar.var_model.VARではこのような時系列データを入力として与えた場合には
        # エラーを出力するようになっている
        # 本ライブラリにおいてエラーの出力を行わないのは、近似的にでも処理結果が欲しいためである
        # また、solverとしてISTA・FISTAを使用する際にも注意が必要である
        # ISTA・FISTAは勾配降下法に似た特徴を有しており、対象の最適化パラメータのスケールに弱い
        # 最適化対象のパラメータの解析解のスケールに依存して、必要な更新回数が多くなる
        # スケールが極端に大きい場合などには事実上収束しないが、そもそも解析解のスケールを事前に知らない・気にしていない場合も多い
        # そのような場合には、教師データ(X, Y)をそれぞれ標準化することで対処できる
        # isStandardization=True に設定しておくことを強く推奨する
        
        if len(self.train_data) <= offset:
            # データ数に対して、オフセットが大き過ぎる
            self.learn_flg = False
            return self.learn_flg
        
        tmp_train_data = self.train_data[offset:]
        nobs           = len(tmp_train_data)
        
        if nobs <= lags:
            # 学習対象データ数に対して、ラグが大き過ぎる
            self.learn_flg = False
            return self.learn_flg
        
        x_data = np.array([tmp_train_data[t-lags : t][::-1].ravel() for t in range(lags, nobs)])
        y_data = tmp_train_data[lags:]
        
        self.lags         = lags
        data_num, expvars = x_data.shape
        _,        objvars = y_data.shape
        
        # 標準化指定の有無
        if self.isStandardization:
            # x軸の標準化
            self.x_mean    = np.mean(x_data, axis=0)
            self.x_std_dev = np.std( x_data, axis=0)
            self.x_std_dev[self.x_std_dev < 1e-32] = 1
            x_data = (x_data - self.x_mean) / self.x_std_dev
            
            # y軸の標準化
            self.y_mean    = np.mean(y_data, axis=0)
            self.y_std_dev = np.std( y_data, axis=0)
            self.y_std_dev[self.y_std_dev < 1e-32] = 1
            y_data = (y_data - self.y_mean) / self.y_std_dev
        else:
            self.x_mean    = np.zeros(expvars)
            self.x_std_dev = np.ones( expvars)
            self.y_mean    = np.zeros(objvars)
            self.y_std_dev = np.ones( objvars)
            
        
        # 本ライブラリで実装されているアルゴリズムは以下の4点となる
        # ・sklearnライブラリに実装されているElasticNet(外部ライブラリ)
        # ・座標降下法アルゴリズム(CD: Coordinate Descent Algorithm)
        # ・メジャライザー最適化( ISTA: Iterative Shrinkage soft-Thresholding Algorithm)
        # ・メジャライザー最適化(FISTA: Fast Iterative Shrinkage soft-Thresholding Algorithm)
        # これらのアルゴリズムは全て同じ目的関数を最適化している
        # しかし、実際に同一のパラメータでパラメータ探索をさせても同一の解は得られない
        # これは、実装の細かな違いによるものであったり、解析解ではなく近似解が得られるためであったりする
        # 特にISTAは勾配降下法と同等の性質を有しているため、異なる近似解が得られる
        # すなわち実行のたびに異なる解が導かれるかつ極所最適解に落ち着くことがある
        # また、外部ライブラリとしてsklearn.linear_model.ElasticNetを利用することもできる
        # この外部ライブラリは内部で座標降下法で探索を行っている点で本ライブラリと同等である
        # 一方で、この外部ライブラリはC言語(Cython)を利用してチューニングが行われている
        # また広く公開され、多くの人に利用されているライブラリでもあるため速度・品質ともにレベルが高い
        # 探索解の品質を保証したいのであれば、外部ライブラリの利用を強く推奨する
        # 一方で、外部ライブラリはデータの標準化処理に対応していない点に注意する必要がある
        # データの標準化処理を行う場合にはL1・L2正則化項の調整を行う必要があるが、外部ライブラリでは行うことができないためである
        # 最後に広く認められているわけではないため使用の際には注意が必要であるが、本ライブラリにて実装済みの
        # これら3種類のアルゴリズムが想定する目的関数は以下のとおり
        # A = 説明変数x + 切片b の行列(データ数n ✖️ (説明変数数s + 1))
        # B = 目的変数y の行列(データ数n ✖️ 目的変数数m)
        # X = 説明変数xの係数 + 切片bの係数 の行列((説明変数数s + 1) ✖️ 目的変数数m)
        # λ_1 = 正則化の強度 * l1_ratio
        # λ_2 = 正則化の強度 * (1 - l1_ratio)
        # math: \begin{equation}
        # math: \begin{split}
        # math: Objective &= \frac{1}{n} \| B - AX \|_2^2 + \frac{λ_2}{2} \| X \|_2^2 + λ_1 \|X\|_1 \\
        # math: &= tr [ \left( B - AX \right) ^T \left( B - AX \right) ] + \frac{λ_2 n}{2} tr [ X^T X ] + λ_1 n \sum_{i=1} |x_i |
        # math: \end{split}
        # math: \end{equation}
        # 参考までに各オプションごとの実行速度は以下の通り
        # external library  >>  FISTA  >>  ISTA  >>  coordinate descent
        
        if   solver == "external library":
            # ElasticNetの外部ライブラリである
            # ラッソ最適化(L1正則化)とリッジ最適化(L2正則化)を行なっている
            # このオプションではsklearnに実装されているモデルに処理を投げることを行なっている
            # 注意点として、データの標準化処理には対応していないことが挙げられる
            # 仮に標準化処理付きでこのオプションが選択された場合には、簡易的な正則化項の調整を行うことにしている
            # しかし、この調整は非常に簡素なものであり厳密性に欠ける
            # このオプションを利用する際には、標準化処理を行わないことを強く推奨する
            tmp_alpha = self.norm_α * np.mean(self.y_std_dev) / np.mean(self.x_std_dev)
            model = ElasticNet(alpha=tmp_alpha.tolist(), l1_ratio=self.l1_ratio.tolist(), max_iter=self.max_iterate, tol=self.tol)
            model.fit(x_data, y_data)
            
            self.alpha, self.alpha0 = model.coef_.T, model.intercept_
            self.alpha0 = self.alpha0.reshape([1, y_data.shape[1]])
            
            if visible_flg:
                l1_norm = self.norm_α * self.l1_ratio       * data_num
                l2_norm = self.norm_α * (1 - self.l1_ratio) * data_num
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                SQUA = np.dot(X.T, X)
                SQUA[objvars-1, objvars-1] = 0
                ABSO = np.abs(X)
                ABSO[expvars, :] = 0
                OBJE = 1 / 2 * np.sum(np.diag(DIFF)) + l2_norm / 2 * np.sum(np.diag(SQUA)) + l1_norm * np.sum(ABSO)
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("L2正則化項(l2 norm):", np.sum(np.diag(SQUA)))
                print("L1正則化項(l1 norm):", np.sum(ABSO))
                print("目的関数(Objective): ", OBJE)
                
                X     = np.vstack([self.alpha, np.zeros([1, objvars])])
                DLoss = np.dot(A.T, B) - l1_norm * np.sign(X) - np.dot(np.dot(A.T, A) + l2_norm * np.identity(expvars + 1), X)
                print("目的関数(Objective)の微分: ", np.abs(DLoss).sum())
            
        elif solver == "coordinate descent":
            # ラッソ最適化(L1正則化)とリッジ最適化(L2正則化)を行なっている
            # 注意点として、切片に対してはラッソ最適化を行わないことが挙げられる
            # リッジ最適化は一般に係数を0にするためではなく、最適化対象のパラメータ全体を小さく保つために利用される
            # 一方で、ラッソ最適化は係数を0にするために利用される手法である
            # そのため、一般にはラッソ最適化を切片に対しては適用しない習慣がある
            # このライブラリもこの習慣に従うことにする
            # リッジ最適化についても切片に対しては適用しないことにした
            # これは標準化を行う前と後で、正則化の効果が変動してしまうことを防ぐためである
            # 実装アルゴリズムは座標降下法である
            # できる限り高速に処理を行いたかったので、このような実装になった
            # このアルゴリズムの計算量は、O(ループ回数 × 説明変数の数 × O(行列積))である
            # 1×M, M×Lの大きさを持つ行列A, Bを想定すると、行列積の計算量はO(ML)となる
            # このSVARライブラリではそれぞれ、M=(説明変数の数 + 1) L=目的変数の数に対応している
            # 計算量オーダーを書き直すと O(ループ回数 × 説明変数の数 × ML)となる
            # このアルゴリズムを利用するにあたって、学習対象データの標準化などの条件は特にない
            # しかし多くの場合において、標準化処理を施してある学習データに対する学習速度は早い
            # その意味で標準化処理を推奨する
            l1_norm = self.norm_α * self.l1_ratio       * data_num
            l2_norm = self.norm_α * (1 - self.l1_ratio) * data_num
            A       = np.hstack([x_data, np.ones([data_num, 1])])
            b       = y_data
            
            L2NORM = l2_norm * np.identity(expvars + 1)
            L2NORM[0:expvars, 0:expvars] = L2NORM[0:expvars, 0:expvars] / np.square(self.x_std_dev.reshape([1, expvars]))
            L2NORM[expvars,   expvars]   = 0
            L = np.dot(A.T, A) + L2NORM
            R = np.dot(A.T, b)
            D = np.diag(np.diag(L))
            G = np.diag(L)
            C = L - D
            
            x_new = np.zeros([expvars + 1, objvars])
            for idx1 in range(0, self.max_iterate):
                x_old = x_new.copy()
                
                x_new[expvars, :] = (R[expvars, :] - np.dot(C[expvars, :], x_new)) / G[expvars]
                for idx2 in range(0, expvars):
                    tmp = R[idx2, :] - np.dot(C[idx2, :], x_new)
                    x_new[idx2, :] = soft_threshold(tmp, l1_norm / self.x_std_dev[idx2] / self.y_std_dev) / G[idx2]
                
                ΔDiff = np.sum((x_new - x_old) ** 2) / data_num
                if visible_flg and (idx1 % 1000 == 0):
                    print(f"ite:{idx1+1}  ΔDiff:{ΔDiff}")
                
                if ΔDiff <= self.tol:
                    break
            
            x = x_new
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                l1_norm = self.norm_α * self.l1_ratio       * data_num
                l2_norm = self.norm_α * (1 - self.l1_ratio) * data_num
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                SQUA = np.dot(X.T, X)
                SQUA[objvars-1, objvars-1] = 0
                ABSO = np.abs(X)
                ABSO[expvars, :] = 0
                OBJE = 1 / 2 * np.sum(np.diag(DIFF)) + l2_norm / 2 * np.sum(np.diag(SQUA)) + l1_norm * np.sum(ABSO)
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("L2正則化項(l2 norm):", np.sum(np.diag(SQUA)))
                print("L1正則化項(l1 norm):", np.sum(ABSO))
                print("目的関数(Objective): ", OBJE)
                
                X     = np.vstack([self.alpha, np.zeros([1, objvars])])
                DLoss = np.dot(A.T, B) - l1_norm * np.sign(X) - np.dot(np.dot(A.T, A) + l2_norm * np.identity(expvars + 1), X)
                print("目的関数(Objective)の微分: ", np.abs(DLoss).sum())
            
        elif solver == "ISTA":
            # ラッソ最適化(L1正則化)とリッジ最適化(L2正則化)を行なっている
            # 注意点として、切片に対してはラッソ最適化を行わないことが挙げられる
            # リッジ最適化は一般に係数を0にするためではなく、最適化対象のパラメータ全体を小さく保つために利用される
            # 一方で、ラッソ最適化は係数を0にするために利用される手法である
            # そのため、一般にはラッソ最適化を切片に対しては適用しない習慣がある
            # このライブラリもこの習慣に従うことにする
            # リッジ最適化についても切片に対しては適用しないことにした
            # これは標準化を行う前と後で、正則化の効果が変動してしまうことを防ぐためである
            # 実装アルゴリズムは一般的なメジャライザー最適化(ISTA: Iterative Shrinkage soft-Thresholding Algorithm)である
            # このアルゴリズムのメジャライザー部分は勾配降下法の更新式に等しい
            # このアルゴリズムを利用する際の注意点として、以下の２つが挙げられる
            # ・教師データ(X, Y)がそれぞれ標準化されている必要があること
            # ・設定イレーション回数が十分でない場合に、大域的最適解への収束が保証できないこと
            # 標準化されていない場合にはうまく収束しないくなる等、アルゴリズムが機能しなくなる可能性がある
            # isStandardization=True に設定しておけば、問題ない
            
            l1_norm      = self.norm_α * self.l1_ratio       * data_num
            l2_norm      = self.norm_α * (1 - self.l1_ratio) * data_num
            A            = np.hstack([x_data, np.ones([data_num, 1])])
            b            = y_data
            L            = np.linalg.norm(A.T.dot(A), ord="fro")
            x_new        = self.random.random([A.shape[1], b.shape[1]])
            l1_specifier = np.ones(x_new.shape)
            l2_specifier = np.ones(x_new.shape)
            l1_specifier[0:expvars, :] = l1_specifier[0:expvars, :] / self.x_std_dev.reshape([expvars, 1])            / self.y_std_dev.reshape([1, objvars])
            l2_specifier[0:expvars, :] = l2_specifier[0:expvars, :] / np.square(self.x_std_dev.reshape([expvars, 1]))
            l1_specifier[expvars,   :] = 0
            l2_specifier[expvars,   :] = 0
            Base_Loss    = 0
            for idx in range(0, self.max_iterate):
                ΔLoss  = b - np.dot(A, x_new)
                ΔDiff  = np.dot(A.T, ΔLoss)
                
                rho    = 1 / L
                diff_x = rho * ΔDiff
                x_new  = soft_threshold(x_new + diff_x, rho * l1_norm * l1_specifier)
                x_new  = x_new / (1 + rho * l2_norm * l2_specifier)
                
                mse = np.sum(ΔLoss ** 2)
                if visible_flg and (idx % 1000 == 0):
                    update_diff = np.sum(diff_x ** 2)
                    print(f"ite:{idx+1}  mse:{mse}  update_diff:{update_diff} diff:{np.abs(Base_Loss - mse)}")
                
                if np.abs(Base_Loss - mse) <= self.tol:
                    break
                else:
                    Base_Loss = mse
            
            x = x_new
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                l1_norm = self.norm_α * self.l1_ratio       * data_num
                l2_norm = self.norm_α * (1 - self.l1_ratio) * data_num
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                SQUA = np.dot(X.T, X)
                SQUA[objvars-1, objvars-1] = 0
                ABSO = np.abs(X)
                ABSO[expvars, :] = 0
                OBJE = 1 / 2 * np.sum(np.diag(DIFF)) + l2_norm / 2 * np.sum(np.diag(SQUA)) + l1_norm * np.sum(ABSO)
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("L2正則化項(l2 norm):", np.sum(np.diag(SQUA)))
                print("L1正則化項(l1 norm):", np.sum(ABSO))
                print("目的関数(Objective): ", OBJE)
                
                X     = np.vstack([self.alpha, np.zeros([1, objvars])])
                DLoss = np.dot(A.T, B) - l1_norm * np.sign(X) - np.dot(np.dot(A.T, A) + l2_norm * np.identity(expvars + 1), X)
                print("目的関数(Objective)の微分: ", np.abs(DLoss).sum())
        
        elif solver == "FISTA":
            # ラッソ最適化(L1正則化)とリッジ最適化(L2正則化)を行なっている
            # 注意点として、切片に対してはラッソ最適化を行わないことが挙げられる
            # リッジ最適化は一般に係数を0にするためではなく、最適化対象のパラメータ全体を小さく保つために利用される
            # 一方で、ラッソ最適化は係数を0にするために利用される手法である
            # そのため、一般にはラッソ最適化を切片に対しては適用しない習慣がある
            # このライブラリもこの習慣に従うことにする
            # リッジ最適化についても切片に対しては適用しないことにした
            # これは標準化を行う前と後で、正則化の効果が変動してしまうことを防ぐためである
            # 実装アルゴリズムは一般的なメジャライザー最適化(FISTA: Fast Iterative Shrinkage soft-Thresholding Algorithm)である
            # このアルゴリズムのメジャライザー部分は勾配降下法の更新式に等しい
            # このアルゴリズムを利用する際の注意点として、以下の２つが挙げられる
            # ・教師データ(X, Y)がそれぞれ標準化されている必要があること
            # ・設定イレーション回数が十分でない場合に、大域的最適解への収束が保証できないこと
            # 標準化されていない場合にはうまく収束しないくなる等、アルゴリズムが機能しなくなる可能性がある
            # isStandardization=True に設定しておけば、問題ない
            
            l1_norm      = self.norm_α * self.l1_ratio       * data_num
            l2_norm      = self.norm_α * (1 - self.l1_ratio) * data_num
            A            = np.hstack([x_data, np.ones([data_num, 1])])
            b            = y_data
            L            = np.linalg.norm(A.T.dot(A), ord="fro")
            x_new        = self.random.random([A.shape[1], b.shape[1]])
            l1_specifier = np.ones(x_new.shape)
            l2_specifier = np.ones(x_new.shape)
            l1_specifier[0:expvars, :] = l1_specifier[0:expvars, :] / self.x_std_dev.reshape([expvars, 1])            / self.y_std_dev.reshape([1, objvars])
            l2_specifier[0:expvars, :] = l2_specifier[0:expvars, :] / np.square(self.x_std_dev.reshape([expvars, 1]))
            l1_specifier[expvars,   :] = 0
            l2_specifier[expvars,   :] = 0
            x_k_m_1      = x_new.copy()
            time_k       = 0
            Base_Loss    = 0
            for idx in range(0, self.max_iterate):
                ΔLoss  = b - np.dot(A, x_new)
                ΔDiff  = np.dot(A.T, ΔLoss)
                
                rho    = 1 / L
                diff_x = rho * ΔDiff
                x_tmp  = soft_threshold(x_new + diff_x, rho * l1_norm * l1_specifier)
                x_tmp  = x_tmp / (1 + rho * l2_norm * l2_specifier)
                
                time_k_a_1 = (1 + np.sqrt(1 + 4 * (time_k ** 2))) / 2
                x_new      = x_tmp + (time_k - 1) / (time_k_a_1) * (x_tmp - x_k_m_1)
                
                time_k  = time_k_a_1
                x_k_m_1 = x_tmp
                
                mse = np.sum(ΔLoss ** 2)
                if visible_flg and (idx % 1000 == 0):
                    update_diff = np.sum(diff_x ** 2)
                    print(f"ite:{idx+1}  mse:{mse}  update_diff:{update_diff} diff:{np.abs(Base_Loss - mse)}")
                
                if (idx != 1) and (np.abs(Base_Loss - mse) <= self.tol):
                    x_new = x_k_m_1
                    break
                else:
                    Base_Loss = mse
            
            x = x_new
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                l1_norm = self.norm_α * self.l1_ratio       * data_num
                l2_norm = self.norm_α * (1 - self.l1_ratio) * data_num
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                SQUA = np.dot(X.T, X)
                SQUA[objvars-1, objvars-1] = 0
                ABSO = np.abs(X)
                ABSO[expvars, :] = 0
                OBJE = 1 / 2 * np.sum(np.diag(DIFF)) + l2_norm / 2 * np.sum(np.diag(SQUA)) + l1_norm * np.sum(ABSO)
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("L2正則化項(l2 norm):", np.sum(np.diag(SQUA)))
                print("L1正則化項(l1 norm):", np.sum(ABSO))
                print("目的関数(Objective): ", OBJE)
                
                X     = np.vstack([self.alpha, np.zeros([1, objvars])])
                DLoss = np.dot(A.T, B) - l1_norm * np.sign(X) - np.dot(np.dot(A.T, A) + l2_norm * np.identity(expvars + 1), X)
                print("目的関数(Objective)の微分: ", np.abs(DLoss).sum())
            
        else:
            raise
        
        # 不偏共分散行列を計算するためには、本来以下のような母数を採用する必要がある
        # 母数 = 学習データ数 - 最適化対象変数の数
        # しかし、このモデルでは”学習データ数 << 最適化対象変数の数”という状況下で利用されることを想定している
        # この状況下では、母数が負の値になってしまうため採用することができない
        # そのため、苦肉の策として 母数 = 学習データ数 - 1 を採用することにした。根拠は弱い。
        # 今後、このような状況での最適な不偏共分散行列の求め方が判明したならば積極的に変更を加えることとする
        denominator    = data_num - 1
        
        self.learn_flg = True
        if self.isStandardization:
            y_pred     = self.predict(x_data * self.x_std_dev + self.x_mean)
            diff       = y_data - (y_pred - self.y_mean) / self.y_std_dev
        else:
            y_pred     = self.predict(x_data)
            diff       = y_data -  y_pred
            
        
        self.sigma     = np.dot(diff.T, diff) / denominator
        self.solver    = solver
        self.data_num  = data_num
        self.unbiased_dispersion = denominator
        self.dispersion          = y_data.shape[0]

        return self.learn_flg

    def predict(self, test_data) -> np.ndarray:
        if type(test_data) is pd.core.frame.DataFrame:
            test_data = test_data.to_numpy()
        
        if type(test_data) is list:
            test_data = np.array(test_data)
        
        if type(test_data) is not np.ndarray:
            print(f"type(test_data) = {type(test_data)}")
            print("エラー：：Numpy型である必要があります。")
            raise
        
        if test_data.ndim != 2:
            print(f"test_data dims = {test_data.ndim}")
            print("エラー：：次元数が一致しません。")
            raise
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        if self.isStandardization:
            test_data = (test_data - self.x_mean) / self.x_std_dev
        
        y_pred = np.dot(test_data, self.alpha) + self.alpha0
        if self.isStandardization:
            y_pred = y_pred * self.y_std_dev + self.y_mean
        
        return y_pred
    
    def get_RSS(self) -> np.ndarray:
        nobs   = len(self.train_data)
        x_data = np.array([self.train_data[t-self.lags : t][::-1].ravel() for t in range(self.lags, nobs)])
        y_data = self.train_data[self.lags:]
        
        y_pred = self.predict(x_data)

        rss = np.square(y_data - y_pred)
        rss = np.sum(rss, axis=0)
        return rss
    
    def get_coefficient(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.alpha0, self.alpha
    
    def log_likelihood(self, offset=0) -> np.float64:
        # なぜか、対数尤度の計算に特殊な計算方法が採用されている
        # statsmodels.tsa.vector_ar.var_model を参照のこと
        # var_loglike関数内にて当該の記述を発見
        # どうやらこれで対数尤度を計算できるらしい
        # math:: -\left(\frac{T}{2}\right) \left(\ln\left|\Omega\right| - K\ln\left(2\pi\right) - K\right)
        # この式が元になっているらしい
        # さっぱり理解できないため、通常通りに計算することにする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        tmp_train_data = self.train_data[offset:]
        nobs           = len(tmp_train_data)
        
        x_data = np.array([tmp_train_data[t-self.lags : t][::-1].ravel() for t in range(self.lags, nobs)])
        y_data = tmp_train_data[self.lags:]
        y_pred = self.predict(x_data)
        
        if self.isStandardization:
            y_data = (y_data - self.y_mean) / self.y_std_dev
            y_pred = (y_pred - self.y_mean) / self.y_std_dev

        # 不偏推定共分散量を通常の推定共分散量に直す
        tmp_sigma      = self.sigma * self.unbiased_dispersion / self.dispersion
        
        log_likelihood = log_likelihood_of_normal_distrubution(y_data.T, y_pred.T, tmp_sigma)
        log_likelihood = np.sum(log_likelihood)

        return log_likelihood
    
    def model_reliability(self, ic:str="aic", offset=0) -> np.float64:
        # statsmodels.tsa.vector_ar.var_model.VARResults を参照のこと
        # info_criteria関数内にて当該の記述を発見
        # 赤池情報基準やベイズ情報基準をはじめとした情報基準が特殊な形に変形されている
        # これは、サンプル数を考慮した改良版らしい
        # これを採用することとする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        num = self.data_num
        k   = self.alpha.size + self.alpha0.size
        
        # caution!!!
        # 本ライブラリでは、データ数に対して最尤推定対象が多い場合にもできる限り処理を続けるように調整してある
        # しかし、この場合に分散共分散行列の正定値性が保てなくなるという問題が発生する
        # また、入力された時系列データ自体に誤りが存在する場合にも正定値性が保てなくなる
        # 正定値行列でない場合には対数尤度の計算ができなくなる
        # この問題の対策のために対数尤度の近似値を求める処理に変更していることに注意
        # 参考URL:
        # https://seetheworld1992.hatenablog.com/entry/2017/03/22/194932
        
        # 対数尤度の計算
        log_likelihood = -2 * self.log_likelihood(offset=offset)

        inf = 0
        if ic == "aic":
            #inf = -2 * log_likelihood + 2 * k
            inf = log_likelihood / num + 2 * k / num
        elif ic == "bic":
            #inf = -2 * log_likelihood + k * np.log(num)
            inf = log_likelihood / num + k * np.log(num) / num
        elif ic == "hqic":
            #inf = -2 * log_likelihood + 2 * k * np.log(np.log(num))
            inf = log_likelihood / num + 2 * k * np.log(np.log(num)) / num
        else:
            raise

        return inf

    def select_order(self, maxlag=15, ic="aic", solver="external library", isVisible=False) -> int:
        if isVisible == True:
            print(f"SVAR model | {ic}", flush=True)
        
        nobs = len(self.train_data)
        if nobs <= maxlag:
            maxlag = nobs - 1

        model_param = []
        for lag in range(1, maxlag + 1):
            flg = self.fit(lags=lag, offset=maxlag - lag, solver=solver)
            
            if flg:
                rel = self.model_reliability(ic=ic, offset=maxlag - lag)
                model_param.append([rel, lag])
            else:
                rel = np.finfo(np.float64).max
                model_param.append([rel, lag])

            if isVisible == True:
                print(f"SVAR({lag}) | {rel}", flush=True)
        
        res_rel, res_lag = np.finfo(np.float64).max, 0
        for elem in model_param:
            tmp_rel, tmp_lag = elem
            if res_rel > tmp_rel:
                res_rel = tmp_rel
                res_lag = tmp_lag
        
        res_lag = res_lag if res_lag != 0 else 1
        self.fit(lags=res_lag, offset=0, solver=solver)
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了しませんでした。")
            raise
        
        if isVisible == True:
            print(f"selected orders | {res_lag}", flush=True)

        return self.lags
    
    def test_causality(self, causing=0, caused=1):
        backup = self.copy()
        tmp_train_data = backup[0]
        tmp_lags       = backup[1]
        tmp_alpha      = backup[2]
        tmp_solver     = backup[13]
        tmp_data_num   = backup[14]

        self.fit(lags=tmp_lags, solver=tmp_solver)
        rss1 = self.get_RSS()[caused]

        caused = caused - 1 if causing < caused else caused
        self.train_data = np.delete(tmp_train_data, causing, axis=1)
        self.fit(lags=tmp_lags, solver=tmp_solver)
        rss0 = self.get_RSS()[caused]

        num    = tmp_train_data.shape[1]
        Fvalue = (rss0 - rss1)/num / (rss1 / (tmp_data_num - tmp_alpha.shape[0] - 1))
        pvalue = stats.chi2.sf(x=Fvalue*num, df=num)

        self.restore(backup)

        return Fvalue*num, pvalue
    
    def ma_replace(self, max=10):
        ma_inf = np.zeros([max + 1, self.train_data.shape[1], self.train_data.shape[1]])
        ma_inf[0, :, :] = np.identity(self.train_data.shape[1])
        
        x_data = ma_inf[0, :, :]
        for _ in range(1, self.lags):
            x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])

        for idx in range(1, max + 1):
            ma_inf[idx, :, :] = np.dot(self.alpha.T, x_data)
            x_data = np.vstack([ma_inf[idx, :, :], x_data[:-self.train_data.shape[1], :]])
        
        self.ma_inf = ma_inf
        return self.ma_inf

    def irf(self, period=30, orth=False, isStdDevShock=True):
        # caution!!!
        # もしもself.isStandardization=Trueであればインパルス応答関数の計算結果は、そのまま解釈することができない
        # なぜならば、標準化されたデータ列に対して推定された係数を元にインパルス応答関数を計算するからである
        # また、標準化の影響を打ち消すような処理を係数やインパルス応答関数の結果そのものに適用する事ができないため
        # 特にデータ列ごとに分散量が1になるように正規化されているので、各次元同士の影響量も比較することができない
        # この場合に有効な見方は「各データ列が正規化されている場合のインパルス応答関数の結果」であって、
        # 「各データ列が生データのままな場合のインパルス応答関数の結果」ではない
        # 他方で、self.isStandardization=Trueであればインパルス応答関数の計算結果が数値として利用価値が低いという訳でもない
        # 各列(各変数)ごとのスケールの違いによる最尤推定量のバイアスが発生せず、変数間の本質的な相互影響量を計算できるためである
        # スケールの復元は行なっているが、標準化されていない場合の計算結果とは一致しないことに注意
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise

        if orth == True:
            A, D = modified_cholesky(self.sigma)

            irf = np.zeros([period + 1, self.train_data.shape[1], self.train_data.shape[1]])
            if isStdDevShock:
                irf[0, :, :] = np.dot(A, np.sqrt(D))
            else:
                irf[0, :, :] = np.dot(A, np.identity(self.train_data.shape[1]))

            x_data = irf[0, :, :]
            for _ in range(1, self.lags):
                x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])
            
            for idx in range(1, period + 1):
                #tmp = self.alpha.reshape(self.lags, self.train_data.shape[1], self.train_data.shape[1])
                #tmp = tmp.swapaxes(1,2).reshape(self.lags * self.train_data.shape[1], self.train_data.shape[1])
                #irf[idx, :, :] = np.dot(x_data, tmp)
                irf[idx, :, :] = np.dot(self.alpha.T, x_data)
                x_data = np.vstack([irf[idx, :, :], x_data[:-self.train_data.shape[1], :]])
            """irf_data = self.irf(period, orth=False)
            L = np.linalg.cholesky(self.sigma)
            irf = np.array([np.dot(coefs, L) for coefs in irf_data])"""

        else:
            irf = self.ma_replace(period)
        
        if self.isStandardization and orth and isStdDevShock:
            irf = irf * self.y_std_dev.T

        return irf
    
    def fevd(self, period=30):
        # caution!!!
        # もしもself.isStandardization=Trueであれば分散分解の計算結果は、そのまま解釈することができない
        # なぜならば、標準化されたデータ列に対して推定された係数を元に分散分解を計算するからである
        # また、標準化の影響を打ち消すような処理を係数や分散分解の結果そのものに適用する事ができないため
        # 特にデータ列ごとに分散量が1になるように正規化されているので、各次元同士の影響量も比較することができない
        # この場合に有効な見方は「各データ列が正規化されている場合の分散分解の結果」であって、
        # 「各データ列が生データのままな場合の分散分解の結果」ではない
        # 他方で、self.isStandardization=Trueであれば分散分解の計算結果が数値として利用価値が低いという訳でもない
        # 各列(各変数)ごとのスケールの違いによる最尤推定量のバイアスが発生せず、変数間の本質的な相互影響量を計算できるためである
        # 分散分解の場合には最終的に1に正規化されるため、スケールの復元はあえて行わないことにする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        # 不偏推定共分散量を通常の推定共分散量に直す
        tmp_sigma = self.sigma * self.unbiased_dispersion / self.dispersion
        A, D = modified_cholesky(tmp_sigma)
        
        fevd = np.zeros([period + 1, self.train_data.shape[1], self.train_data.shape[1]])
        fevd[0, :, :] = A
        
        x_data = fevd[0, :, :]
        for _ in range(1, self.lags):
            x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])
        
        fevd[0, :, :] = fevd[0, :, :] ** 2
        for idx in range(1, period + 1):
            fevd[idx, :, :] = np.dot(self.alpha.T, x_data)
            x_data = np.vstack([fevd[idx, :, :], x_data[:-self.train_data.shape[1], :]])
            
            fevd[idx, :, :] = fevd[idx, :, :] ** 2
        
        fevd = fevd.cumsum(axis=0)
        for idx in range(0, period + 1):
            fevd[idx, :, :] = np.dot(fevd[idx, :, :], D)
        
        for idx in range(0, period + 1):
            fevd[idx, :, :] = fevd[idx, :, :] / np.sum(fevd[idx, :, :], axis=1).reshape([self.train_data.shape[1], 1])
        """fevd = self.irf(period=period, orth=True)
        fevd = (fevd ** 2).cumsum(axis=0)
        for idx in range(0, period + 1):
            fevd[idx, :, :] = fevd[idx, :, :] / np.sum(fevd[idx, :, :], axis=1).reshape([self.train_data.shape[1], 1])"""
        
        return fevd


class Non_Negative_Vector_Auto_Regressive:
    def __init__(self,
                 train_data,                         # 学習対象時系列データ
                 tol:float=1e-6,                     # 許容誤差
                 isStandardization:bool=True,        # 標準化処理の適用有無
                 max_iterate:int=300000,             # 最大ループ回数
                 random_state=None) -> None:         # 乱数のシード値
        if type(train_data) is pd.core.frame.DataFrame:
            train_data = train_data.to_numpy()
        
        if type(train_data) is list:
            train_data = np.array(train_data)
        
        if type(train_data) is not np.ndarray:
            print(f"type(train_data) = {type(train_data)}")
            print("エラー：：Numpy型である必要があります。")
            raise
        
        if train_data.ndim != 2:
            print(f"train_data dims = {train_data.ndim}")
            print("エラー：：次元数が一致しません。")
            raise
        
        self.train_data          = train_data
        self.lags                = 0
        self.alpha               = np.zeros([1, 1])
        self.alpha0              = np.zeros([1, 1])
        self.sigma               = np.zeros([1, 1])
        self.isStandardization   = isStandardization
        self.x_mean              = np.zeros([1, 1])
        self.x_std_dev           = np.ones([1, 1])
        self.y_mean              = np.zeros([1, 1])
        self.y_std_dev           = np.ones([1, 1])
        self.tol                 = tol
        self.solver              = ""
        self.data_num            = 0
        self.max_iterate         = round(max_iterate)
        self.unbiased_dispersion = 0
        self.dispersion          = 0
        self.ma_inf              = np.zeros([1, 1])
        self.learn_flg           = False

        self.random_state = random_state
        if random_state != None:
            self.random = np.random
            self.random.seed(seed=self.random_state)
        else:
            self.random = np.random
            
    def copy(self):
        buf = []
        buf = buf + [self.train_data.copy()]
        buf = buf + [self.lags]
        buf = buf + [self.alpha.copy()]
        buf = buf + [self.alpha0.copy()]
        buf = buf + [self.sigma.copy()]
        buf = buf + [self.isStandardization]
        buf = buf + [self.x_mean]
        buf = buf + [self.x_std_dev]
        buf = buf + [self.y_mean]
        buf = buf + [self.y_std_dev]
        buf = buf + [self.tol]
        buf = buf + [self.solver]
        buf = buf + [self.data_num]
        buf = buf + [self.max_iterate]
        buf = buf + [self.unbiased_dispersion]
        buf = buf + [self.dispersion]
        buf = buf + [self.ma_inf.copy()]
        buf = buf + [self.learn_flg]
        buf = buf + [self.random_state]
        buf = buf + [self.random]
        
        return buf
    
    def restore(self, buf):
        self.train_data          = buf[0]
        self.lags                = buf[1]
        self.alpha               = buf[2]
        self.alpha0              = buf[3]
        self.sigma               = buf[4]
        self.isStandardization   = buf[5]
        self.x_mean              = buf[6]
        self.x_std_dev           = buf[7]
        self.y_mean              = buf[8]
        self.y_std_dev           = buf[9]
        self.tol                 = buf[10]
        self.solver              = buf[11]
        self.data_num            = buf[12]
        self.max_iterate         = buf[13]
        self.unbiased_dispersion = buf[14]
        self.dispersion          = buf[15]
        self.ma_inf              = buf[16]
        self.learn_flg           = buf[17]
        self.random_state        = buf[18]
        self.random              = buf[19]
        
        return True

    def fit(self, lags:int=1, offset:int=0, solver="Optimizer Rafael", visible_flg:bool=False) -> bool:
        # caution!!!
        # solverとして最急降下法を使用する際に注意が必要である
        # 勾配降下法は、対象の最適化パラメータのスケールに弱い
        # 最適化対象のパラメータの解析解のスケールに依存して、必要な更新回数が多くなる
        # スケールが極端に大きい場合などには事実上収束しないが、そもそも解析解のスケールを事前に知らない・気にしていない場合も多い
        # そのような場合には、教師データ(X, Y)をそれぞれ標準化することで対処できる
        # isStandardization=True に設定しておくことを強く推奨する
        
        if len(self.train_data) <= offset:
            # データ数に対して、オフセットが大き過ぎる
            self.learn_flg = False
            return self.learn_flg
        
        tmp_train_data = self.train_data[offset:]
        nobs           = len(tmp_train_data)
        
        if nobs <= lags:
            # 学習対象データ数に対して、ラグが大き過ぎる
            self.learn_flg = False
            return self.learn_flg
        
        x_data = np.array([tmp_train_data[t-lags : t][::-1].ravel() for t in range(lags, nobs)])
        y_data = tmp_train_data[lags:]
        
        self.lags         = lags
        data_num, expvars = x_data.shape
        _,        objvars = y_data.shape
        
        # 標準化指定の有無
        if self.isStandardization:
            # 非負制約付きのVARモデルでは、y軸方向の標準化にアレンジを加えてある
            # 具体的には、平均値から2.58*標準偏差を引いた値を採用することにしている
            # 2.58という係数を採用した理由については、正規分布において有意水準1%(片側0.5%)であるからである
            # 実際には、y軸方向のデータセットが何の確率分布にしたがっているかは不明であるため注意が必要である
            # さて、なぜこのような値を新しい平均値として採用したかというと学習パラメータのスケールを大きくしたかったからという点が挙げられる
            # そもそも非負制約付きVARモデルを採用した理由は、学習後のパラメータ同士の比較を行いたかったからである
            # しかし、一般的な標準化方式では切片パラメータが非常に0に近しくなり比較がやりにくくなる
            # そこで、x軸方向のデータセットの大きさを制約したままy軸方向のデータセットの制約を緩くすることで、切片を含む全ての学習パラメータのスケールを大きくする
            # 具体的には以下のような数式イメージを想定している
            # X = 説明変数x の行列(説明変数数expvars ✖️ 目的変数数objvars)
            # Y = 目的変数y の行列(データ数data_num ✖️ 目的変数数objvars)
            # A = 係数の行列(説明変数数expvars ✖️ 目的変数数objvars)
            # b = 切片のベクトル(目的変数数objvars)
            # math: \begin{equation}
            # math: \begin{split}
            # math: Y_mi &= A_1i X_1i + A_2i X_2i + A_3i X_3i + ... + A_{expvars i} X_{expvars i} + b_i
            # math: \end{split}
            # math: \end{equation}
            # ここで、一般的な標準化処理を施すと以下のように解釈できる
            # x = 平均0 分散1 のランダム定数ベクトル
            # y = 平均0 分散1 のランダム定数
            # a = 学習パラメータベクトル(非負制約)
            # b = 学習パラメータ定数(非負制約)
            # math: \begin{equation}
            # math: \begin{split}
            # math: y &= a_1 x_1 + a_2 x_2 + a_3 x_3 + ... + a_expvars x_expvars + b
            # math: \end{split}
            # math: \end{equation}
            # この上記の数式より各x成分のスケールとy値のスケールが一致していることがわかる
            # この事実は各学習パラメータのスケール ≒ 1 かつ b ≒ 0 であることを意味しており、係数の比較を行う上で不都合である
            # なぜならば、スケール比が1:1であると最尤推定時にパラメータのスケールに対して鈍感となるためである
            # この性質は「学習パラメータ(非負な値)同士を比較する」という目的にそぐわなくなる
            # そのためy値を 平均c 分散1 のランダム定数に変換することで、各x成分のスケール << y値のスケール を保証する
            # これにより各学習パラメータのスケールをy値のスケールに合わせる必要が出てくるため、最尤推定時にパラメータのスケールに対して敏感となる
            # 参考に標準正規分布の確立分布　 数表を掲載する
            # URL:https://kyozaikenkyu-statistics.blog.jp/%E6%A8%99%E6%BA%96%E6%AD%A3%E8%A6%8F%E5%88%86%E5%B8%83%E6%95%B0%E8%A1%A8.pdf
            # 主に以下の値が利用されると想定する
            # 優位水準30%  (片側15.0%)・・・1.04
            # 優位水準20%  (片側10.0%)・・・1.28
            # 優位水準10%  (片側5.0%) ・・・1.64
            # 優位水準5%   (片側2.5%) ・・・1.96
            # 優位水準3%   (片側1.5%) ・・・2.17
            # 優位水準1%   (片側0.5%) ・・・2.58
            
            # x軸の標準化
            self.x_mean    = np.mean(x_data, axis=0)
            self.x_std_dev = np.std( x_data, axis=0)
            self.x_std_dev[self.x_std_dev < 1e-32] = 1
            x_data = (x_data - self.x_mean) / self.x_std_dev
            
            # y軸の標準化
            self.y_mean    = np.mean(y_data, axis=0) - 1.28 * np.std( y_data, axis=0)
            self.y_std_dev = np.std( y_data, axis=0)
            self.y_std_dev[self.y_std_dev < 1e-32] = 1
            y_data = (y_data - self.y_mean) / self.y_std_dev
        else:
            self.x_mean    = np.zeros(expvars)
            self.x_std_dev = np.ones( expvars)
            self.y_mean    = np.zeros(objvars)
            self.y_std_dev = np.ones( objvars)

        
        # 本ライブラリで実装されているアルゴリズムは以下の3点となる
        # ・勾配降下法(GD: Gradient Descent)
        # ・ネステロフの加速勾配法(NAG: Nesterov Accelerateed Gradient)
        # ・適応的勾配降下法(AGD: Adaptive Gradient Descent(Rafeal))
        # これらのアルゴリズムは全て同じ目的関数を最適化している
        # 広く認められているわけではないため使用の際には注意が必要であるが、本ライブラリにて実装済みの
        # これら2種類のアルゴリズムが想定する目的関数は以下のとおり
        # A = 説明変数x + 切片b の行列(データ数n ✖️ (説明変数数s + 1))
        # B = 目的変数y の行列(データ数n ✖️ 目的変数数m)
        # X = 説明変数xの係数 + 切片bの係数 の行列((説明変数数s + 1) ✖️ 目的変数数m)
        # math: \begin{equation}
        # math: \begin{split}
        # math: Objective &= \frac{1}{n} \| B - AX \|_2^2 \\
        # math: &= tr [ \left( B - AX \right) ^T \left( B - AX \right) ]
        # math: \end{split}
        # math: \end{equation}
        # 参考までに各オプションごとの実行速度は以下の通り
        # AGD  >>  NAG  >>  GD


        if   solver == "Gradient Descent":
            A            = np.hstack([x_data, np.ones([data_num, 1])])
            b            = y_data
            L            = np.linalg.norm(A.T.dot(A), ord="fro")
            x_new        = self.random.random([A.shape[1], b.shape[1]])
            for idx in range(0, self.max_iterate):
                ΔSQUA  = np.square(x_new) / 2
                ΔLoss  = b - np.dot(A, ΔSQUA)
                ΔDiff  = np.dot(A.T, ΔLoss) * x_new
                
                rho    = 1 / L
                diff_x = rho * ΔDiff
                x_new  = x_new + diff_x
                
                update_diff = np.max(np.abs(ΔDiff))
                if visible_flg and (idx % 1000 == 0):
                    print(f"ite:{idx+1}  ΔLoss:{np.max(np.abs(ΔLoss))}  update_diff:{update_diff}")
                
                if update_diff <= self.tol:
                    break
            
            x = np.square(x_new) / 2
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                OBJE = 1 / 2 * np.sum(np.diag(DIFF))
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("目的関数(Objective): ", OBJE)
        
        elif solver == "Nesterov Accelerateed Gradient":
            A            = np.hstack([x_data, np.ones([data_num, 1])])
            b            = y_data
            L            = np.linalg.norm(A.T.dot(A), ord="fro")
            x_new        = self.random.random([A.shape[1], b.shape[1]])
            x_k_m_1      = x_new.copy()
            time_k       = 0
            for idx in range(0, self.max_iterate):
                ΔSQUA  = np.square(x_new) / 2
                ΔLoss  = b - np.dot(A, ΔSQUA)
                ΔDiff  = np.dot(A.T, ΔLoss) * x_new
                
                rho    = 1 / L
                diff_x = rho * ΔDiff
                x_tmp  = x_new + diff_x
                
                time_k_a_1 = (1 + np.sqrt(1 + 4 * (time_k ** 2))) / 2
                x_new      = x_tmp + (time_k - 1) / (time_k_a_1) * (x_tmp - x_k_m_1)
                
                time_k  = time_k_a_1
                x_k_m_1 = x_tmp
                
                update_diff = np.max(np.abs(ΔDiff))
                if visible_flg and (idx % 1000 == 0):
                    print(f"ite:{idx+1}  ΔLoss:{np.max(np.abs(ΔLoss))}  update_diff:{update_diff}")
                
                if (idx != 1) and (update_diff <= self.tol):
                    x_new = x_k_m_1
                    break
            
            x = np.square(x_new) / 2
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                OBJE = 1 / 2 * np.sum(np.diag(DIFF))
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("目的関数(Objective): ", OBJE)
        
        elif solver == "Optimizer Rafael":
            A            = np.hstack([x_data, np.ones([data_num, 1])])
            b            = y_data
            x_new        = self.random.random([A.shape[1], b.shape[1]])
            Optimizer    = Update_Rafael(0.1, beta=0.9, isSHC=False)
            for idx in range(0, self.max_iterate):
                ΔSQUA  = np.square(x_new) / 2
                ΔLoss  = b - np.dot(A, ΔSQUA)
                ΔDiff  = np.dot(A.T, ΔLoss) * x_new
                
                diff_x = Optimizer.update(ΔDiff)
                x_new  = x_new + diff_x
                
                update_diff = np.max(np.abs(ΔDiff))
                if visible_flg and (idx % 1000 == 0):
                    print(f"ite:{idx+1}  ΔLoss:{np.max(np.abs(ΔLoss))}  update_diff:{update_diff}")
                
                if update_diff <= self.tol:
                    break
            
            x = np.square(x_new) / 2
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                OBJE = 1 / 2 * np.sum(np.diag(DIFF))
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("目的関数(Objective): ", OBJE)
        
        elif solver == "Augmented Lagrangians Method":
            A          = np.hstack([x_data, np.ones([data_num, 1])])
            b          = y_data
            slack      = np.ones([A.shape[1], b.shape[1]])
            u_lagrange = np.zeros([A.shape[1], b.shape[1]])
            x_new      = self.random.random([A.shape[1], b.shape[1]])
            ρ          = 1
            lr         = 0.01
            for idx1 in range(0, self.max_iterate):
                Optimizer_X = Update_Rafael(lr, beta=0.99, isSHC=False)
                Optimizer_S = Update_Rafael(lr, beta=0.99, isSHC=False)
                for idx2 in range(0, self.max_iterate):
                    ΔLoss = b - np.dot(A, x_new)
                    ΔDX   = -np.dot(A.T, ΔLoss) - u_lagrange - ρ * (np.square(slack) / 2 - x_new)
                    ΔDS   = u_lagrange * slack + ρ * slack * (np.square(slack) / 2 - x_new)
                
                    diff_x = ΔDX
                    x_new  = x_new - Optimizer_X.update(diff_x)
                    diff_s = ΔDS
                    slack  = slack - Optimizer_S.update(diff_s)

                    update_diff = np.max(np.abs(diff_x))
                    if visible_flg and (idx2 % 1000 == 0):
                        update_size = np.sum(diff_x ** 2 + diff_s ** 2)
                        error       = ρ * np.max((np.square(slack) / 2 - x_new) ** 2)
                        print(f"ite1:{idx1+1} ite2:{idx2+1}  error:{error}  ΔLoss^2:{np.sum(ΔLoss ** 2)}  update_size:{update_size} update_diff:{update_diff}")
                
                    if update_diff <= lr:
                        break
                
                u_lagrange = u_lagrange + ρ * (np.square(slack) / 2 - x_new)
                ρ          = ρ * 3
                
                error = ρ * np.max((np.square(slack) / 2 - x_new) ** 2)
                if error <= self.tol:
                    break
            
            x = np.square(slack) / 2
            self.alpha, self.alpha0 = x[0:expvars, :], x[expvars, :]
            self.alpha0 = self.alpha0.reshape([1, x.shape[1]])
            
            if visible_flg:
                A       = np.hstack([x_data, np.ones([data_num, 1])])
                B       = y_data
                X       = np.vstack([self.alpha, self.alpha0])
                DIFF = B - np.dot(A, X)
                DIFF = np.dot(DIFF.T, DIFF)
                OBJE = 1 / 2 * np.sum(np.diag(DIFF))
                print("平均二乗誤差(MSE):", np.sum(np.diag(DIFF)) / data_num, flush=True)
                print("目的関数(Objective): ", OBJE)
        
        else:
            raise
        
        # 不偏共分散行列を計算するためには、本来以下のような母数を採用する必要がある
        # 母数 = 学習データ数 - 最適化対象変数の数
        # しかし、このモデルでは”学習データ数 << 最適化対象変数の数”という状況下で利用されることを想定している
        # この状況下では、母数が負の値になってしまうため採用することができない
        # そのため、苦肉の策として 母数 = 学習データ数 - 1 を採用することにした。根拠は弱い。
        # 今後、このような状況での最適な不偏共分散行列の求め方が判明したならば積極的に変更を加えることとする
        denominator    = data_num - 1
        
        self.learn_flg = True
        if self.isStandardization:
            y_pred     = self.predict(x_data * self.x_std_dev + self.x_mean)
            diff       = y_data - (y_pred - self.y_mean) / self.y_std_dev
        else:
            y_pred     = self.predict(x_data)
            diff       = y_data -  y_pred
            
        
        self.sigma     = np.dot(diff.T, diff) / denominator
        self.solver    = solver
        self.data_num  = data_num
        self.unbiased_dispersion = denominator
        self.dispersion          = y_data.shape[0]

        return self.learn_flg

    def predict(self, test_data) -> np.ndarray:
        if type(test_data) is pd.core.frame.DataFrame:
            test_data = test_data.to_numpy()
        
        if type(test_data) is list:
            test_data = np.array(test_data)
        
        if type(test_data) is not np.ndarray:
            print(f"type(test_data) = {type(test_data)}")
            print("エラー：：Numpy型である必要があります。")
            raise
        
        if test_data.ndim != 2:
            print(f"test_data dims = {test_data.ndim}")
            print("エラー：：次元数が一致しません。")
            raise
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        if self.isStandardization:
            test_data = (test_data - self.x_mean) / self.x_std_dev
        
        y_pred = np.dot(test_data, self.alpha) + self.alpha0
        if self.isStandardization:
            y_pred = y_pred * self.y_std_dev + self.y_mean
        
        return y_pred
    
    def get_RSS(self) -> np.ndarray:
        nobs   = len(self.train_data)
        x_data = np.array([self.train_data[t-self.lags : t][::-1].ravel() for t in range(self.lags, nobs)])
        y_data = self.train_data[self.lags:]
        
        y_pred = self.predict(x_data)

        rss = np.square(y_data - y_pred)
        rss = np.sum(rss, axis=0)
        return rss
    
    def get_coefficient(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.alpha0, self.alpha
    
    def log_likelihood(self, offset=0) -> np.float64:
        # なぜか、対数尤度の計算に特殊な計算方法が採用されている
        # statsmodels.tsa.vector_ar.var_model を参照のこと
        # var_loglike関数内にて当該の記述を発見
        # どうやらこれで対数尤度を計算できるらしい
        # math:: -\left(\frac{T}{2}\right) \left(\ln\left|\Omega\right| - K\ln\left(2\pi\right) - K\right)
        # この式が元になっているらしい
        # さっぱり理解できないため、通常通りに計算することにする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        tmp_train_data = self.train_data[offset:]
        nobs           = len(tmp_train_data)
        
        x_data = np.array([tmp_train_data[t-self.lags : t][::-1].ravel() for t in range(self.lags, nobs)])
        y_data = tmp_train_data[self.lags:]
        y_pred = self.predict(x_data)
        
        if self.isStandardization:
            y_data = (y_data - self.y_mean) / self.y_std_dev
            y_pred = (y_pred - self.y_mean) / self.y_std_dev

        # 不偏推定共分散量を通常の推定共分散量に直す
        tmp_sigma      = self.sigma * self.unbiased_dispersion / self.dispersion
        
        log_likelihood = log_likelihood_of_normal_distrubution(y_data.T, y_pred.T, tmp_sigma)
        log_likelihood = np.sum(log_likelihood)

        return log_likelihood
    
    def model_reliability(self, ic:str="aic", offset=0) -> np.float64:
        # statsmodels.tsa.vector_ar.var_model.VARResults を参照のこと
        # info_criteria関数内にて当該の記述を発見
        # 赤池情報基準やベイズ情報基準をはじめとした情報基準が特殊な形に変形されている
        # これは、サンプル数を考慮した改良版らしい
        # これを採用することとする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        num = self.data_num
        k   = self.alpha.size + self.alpha0.size
        
        # caution!!!
        # 本ライブラリでは、データ数に対して最尤推定対象が多い場合にもできる限り処理を続けるように調整してある
        # しかし、この場合に分散共分散行列の正定値性が保てなくなるという問題が発生する
        # また、入力された時系列データ自体に誤りが存在する場合にも正定値性が保てなくなる
        # 正定値行列でない場合には対数尤度の計算ができなくなる
        # この問題の対策のために対数尤度の近似値を求める処理に変更していることに注意
        # 参考URL:
        # https://seetheworld1992.hatenablog.com/entry/2017/03/22/194932
        
        # 対数尤度の計算
        log_likelihood = -2 * self.log_likelihood(offset=offset)

        inf = 0
        if ic == "aic":
            #inf = -2 * log_likelihood + 2 * k
            inf = log_likelihood / num + 2 * k / num
        elif ic == "bic":
            #inf = -2 * log_likelihood + k * np.log(num)
            inf = log_likelihood / num + k * np.log(num) / num
        elif ic == "hqic":
            #inf = -2 * log_likelihood + 2 * k * np.log(np.log(num))
            inf = log_likelihood / num + 2 * k * np.log(np.log(num)) / num
        else:
            raise

        return inf

    def select_order(self, maxlag=15, ic="aic", solver="Optimizer Rafael", isVisible=False) -> int:
        if isVisible == True:
            print(f"SVAR model | {ic}", flush=True)
        
        nobs = len(self.train_data)
        if nobs <= maxlag:
            maxlag = nobs - 1

        model_param = []
        for lag in range(1, maxlag + 1):
            flg = self.fit(lags=lag, offset=maxlag - lag, solver=solver)
            
            if flg:
                rel = self.model_reliability(ic=ic, offset=maxlag - lag)
                model_param.append([rel, lag])
            else:
                rel = np.finfo(np.float64).max
                model_param.append([rel, lag])

            if isVisible == True:
                print(f"SVAR({lag}) | {rel}", flush=True)
        
        res_rel, res_lag = np.finfo(np.float64).max, 0
        for elem in model_param:
            tmp_rel, tmp_lag = elem
            if res_rel > tmp_rel:
                res_rel = tmp_rel
                res_lag = tmp_lag
        
        res_lag = res_lag if res_lag != 0 else 1
        self.fit(lags=res_lag, offset=0, solver=solver)
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了しませんでした。")
            raise
        
        if isVisible == True:
            print(f"selected orders | {res_lag}", flush=True)

        return self.lags
    
    def test_causality(self, causing=0, caused=1):
        backup = self.copy()
        tmp_train_data = backup[0]
        tmp_lags       = backup[1]
        tmp_alpha      = backup[2]
        tmp_solver     = backup[11]
        tmp_data_num   = backup[12]

        self.fit(lags=tmp_lags, solver=tmp_solver)
        rss1 = self.get_RSS()[caused]

        caused = caused - 1 if causing < caused else caused
        self.train_data = np.delete(tmp_train_data, causing, axis=1)
        self.fit(lags=tmp_lags, solver=tmp_solver)
        rss0 = self.get_RSS()[caused]

        num    = tmp_train_data.shape[1]
        Fvalue = (rss0 - rss1)/num / (rss1 / (tmp_data_num - tmp_alpha.shape[0] - 1))
        pvalue = stats.chi2.sf(x=Fvalue*num, df=num)

        self.restore(backup)

        return Fvalue*num, pvalue
    
    def ma_replace(self, max=10):
        ma_inf = np.zeros([max + 1, self.train_data.shape[1], self.train_data.shape[1]])
        ma_inf[0, :, :] = np.identity(self.train_data.shape[1])
        
        x_data = ma_inf[0, :, :]
        for _ in range(1, self.lags):
            x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])

        for idx in range(1, max + 1):
            ma_inf[idx, :, :] = np.dot(self.alpha.T, x_data)
            x_data = np.vstack([ma_inf[idx, :, :], x_data[:-self.train_data.shape[1], :]])
        
        self.ma_inf = ma_inf
        return self.ma_inf

    def irf(self, period=30, orth=False, isStdDevShock=True):
        # caution!!!
        # もしもself.isStandardization=Trueであればインパルス応答関数の計算結果は、そのまま解釈することができない
        # なぜならば、標準化されたデータ列に対して推定された係数を元にインパルス応答関数を計算するからである
        # また、標準化の影響を打ち消すような処理を係数やインパルス応答関数の結果そのものに適用する事ができないため
        # 特にデータ列ごとに分散量が1になるように正規化されているので、各次元同士の影響量も比較することができない
        # この場合に有効な見方は「各データ列が正規化されている場合のインパルス応答関数の結果」であって、
        # 「各データ列が生データのままな場合のインパルス応答関数の結果」ではない
        # 他方で、self.isStandardization=Trueであればインパルス応答関数の計算結果が数値として利用価値が低いという訳でもない
        # 各列(各変数)ごとのスケールの違いによる最尤推定量のバイアスが発生せず、変数間の本質的な相互影響量を計算できるためである
        # スケールの復元は行なっているが、標準化されていない場合の計算結果とは一致しないことに注意
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise

        if orth == True:
            A, D = modified_cholesky(self.sigma)

            irf = np.zeros([period + 1, self.train_data.shape[1], self.train_data.shape[1]])
            if isStdDevShock:
                irf[0, :, :] = np.dot(A, np.sqrt(D))
            else:
                irf[0, :, :] = np.dot(A, np.identity(self.train_data.shape[1]))

            x_data = irf[0, :, :]
            for _ in range(1, self.lags):
                x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])
            
            for idx in range(1, period + 1):
                #tmp = self.alpha.reshape(self.lags, self.train_data.shape[1], self.train_data.shape[1])
                #tmp = tmp.swapaxes(1,2).reshape(self.lags * self.train_data.shape[1], self.train_data.shape[1])
                #irf[idx, :, :] = np.dot(x_data, tmp)
                irf[idx, :, :] = np.dot(self.alpha.T, x_data)
                x_data = np.vstack([irf[idx, :, :], x_data[:-self.train_data.shape[1], :]])
            """irf_data = self.irf(period, orth=False)
            L = np.linalg.cholesky(self.sigma)
            irf = np.array([np.dot(coefs, L) for coefs in irf_data])"""

        else:
            irf = self.ma_replace(period)
        
        if self.isStandardization and orth and isStdDevShock:
            irf = irf * self.y_std_dev.T

        return irf
    
    def fevd(self, period=30):
        # caution!!!
        # もしもself.isStandardization=Trueであれば分散分解の計算結果は、そのまま解釈することができない
        # なぜならば、標準化されたデータ列に対して推定された係数を元に分散分解を計算するからである
        # また、標準化の影響を打ち消すような処理を係数や分散分解の結果そのものに適用する事ができないため
        # 特にデータ列ごとに分散量が1になるように正規化されているので、各次元同士の影響量も比較することができない
        # この場合に有効な見方は「各データ列が正規化されている場合の分散分解の結果」であって、
        # 「各データ列が生データのままな場合の分散分解の結果」ではない
        # 他方で、self.isStandardization=Trueであれば分散分解の計算結果が数値として利用価値が低いという訳でもない
        # 各列(各変数)ごとのスケールの違いによる最尤推定量のバイアスが発生せず、変数間の本質的な相互影響量を計算できるためである
        # 分散分解の場合には最終的に1に正規化されるため、スケールの復元はあえて行わないことにする
        
        if not self.learn_flg:
            print(f"learn_flg = {self.learn_flg}")
            print("エラー：：学習が完了していません。")
            raise
        
        # 不偏推定共分散量を通常の推定共分散量に直す
        tmp_sigma = self.sigma * self.unbiased_dispersion / self.dispersion
        A, D = modified_cholesky(tmp_sigma)
        
        fevd = np.zeros([period + 1, self.train_data.shape[1], self.train_data.shape[1]])
        fevd[0, :, :] = A
        
        x_data = fevd[0, :, :]
        for _ in range(1, self.lags):
            x_data = np.vstack([x_data, np.zeros([self.train_data.shape[1], self.train_data.shape[1]])])
        
        fevd[0, :, :] = fevd[0, :, :] ** 2
        for idx in range(1, period + 1):
            fevd[idx, :, :] = np.dot(self.alpha.T, x_data)
            x_data = np.vstack([fevd[idx, :, :], x_data[:-self.train_data.shape[1], :]])
            
            fevd[idx, :, :] = fevd[idx, :, :] ** 2
        
        fevd = fevd.cumsum(axis=0)
        for idx in range(0, period + 1):
            fevd[idx, :, :] = np.dot(fevd[idx, :, :], D)
        
        for idx in range(0, period + 1):
            fevd[idx, :, :] = fevd[idx, :, :] / np.sum(fevd[idx, :, :], axis=1).reshape([self.train_data.shape[1], 1])
        """fevd = self.irf(period=period, orth=True)
        fevd = (fevd ** 2).cumsum(axis=0)
        for idx in range(0, period + 1):
            fevd[idx, :, :] = fevd[idx, :, :] / np.sum(fevd[idx, :, :], axis=1).reshape([self.train_data.shape[1], 1])"""
        
        return fevd