from Utils.DBManager import DBManager
import pandas as pd
import numpy as np
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
from datetime import datetime
from TSE.TSEConnection import TSEConnection
from config import DB_SERVER, ROOT_DIR, DB_NAME
from Utils.Utils import create_logger

items_to_get_intra_day = ['client_type', 'static_threshold', 'closing_price', 'intratrade', 'best_limit']


def get_history_daily_data(start_date=None):
    logger = create_logger()
    logger.info('=== history daily tse ===')

    db_manager = DBManager(DB_SERVER, DB_NAME)

    df_stocks = db_manager.get_from_collection(
        {'ind_matlab_stock': {'$exists': True}, 'has_started': True, 'is_ignored': False}, 'stocks')
    df_stocks = df_stocks.sort_values(by='co_id').reset_index()

    df_dates = db_manager.get_from_collection({}, 'dates')
    df_dates = df_dates.sort_values(by='date', ascending=True)

    tse_con = TSEConnection()

    columns_daily = ['date', 'high', 'low', 'vwap', 'close', 'open', 'vwap_prev', 'value', 'volume', 'num_trades']

    list_to_get = []
    for index_stock, row_stock in df_stocks.iterrows():
        list_to_get.append((row_stock,))

    pbar = tqdm(total=len(df_stocks))

    def _get_daily_data(row_stock):
        pbar.update(1)
        ids_history = row_stock['ids_history']
        df_daily = pd.DataFrame()
        for id_hist in ids_history:
            if (pd.isna(id_hist['start_date'])) or (
                    start_date is not None and id_hist['stop_date'] is not None and id_hist['stop_date'] < start_date[
                'date']):
                continue
            tse_id = id_hist['tse_id']

            df_temp = tse_con.get_history_daily(tse_id)
            if df_temp is not None:
                df_temp.columns = columns_daily
                df_temp['tse_id'] = tse_id
                df_temp = df_temp.dropna()
                df_temp[['volume', 'value']] = df_temp[['volume', 'value']].astype(float)
                df_temp = df_temp.loc[df_temp['volume'] > 0].copy()
                df_daily = pd.concat([df_daily, df_temp])

        if len(df_daily) > 0:
            df_daily['date'] = df_daily['date'].apply(lambda x: datetime.strptime(x, '%Y%m%d'))
            df_daily = df_daily.sort_values(by=['date']).reset_index(drop=True)

            df_daily = pd.merge(df_daily, df_dates, how='right', on='date')
            df_daily['co_id'] = row_stock['co_id']
            df_daily['symbol'] = row_stock['symbol']
            df_daily['ind_matlab_stock'] = row_stock['ind_matlab_stock']

            df_daily[['high', 'low', 'close', 'vwap', 'open', 'vwap_prev', 'num_trades']] = df_daily[
                ['high', 'low', 'close', 'vwap', 'open', 'vwap_prev', 'num_trades']].astype(float)

            df_daily['vwap'] = df_daily['vwap'].fillna(method='ffill')
            del df_daily['_id']

            d_last_day = df_daily.loc[len(df_daily) - 1].to_dict()
            info_last = db_manager.get_from_collection({'co_id': row_stock['co_id'], 'date': d_last_day['date']},
                                                       'inst_info', return_list=True)
            if len(info_last) > 0:
                info_last = info_last[0]
                d_last_day['high'] = info_last['high']
                d_last_day['low'] = info_last['low']
                d_last_day['vwap'] = info_last['vwap']
                d_last_day['close'] = info_last['close']
                d_last_day['open'] = info_last['open']
                d_last_day['vwap_prev'] = info_last['vwap_prev']
                d_last_day['value'] = info_last['value']
                d_last_day['volume'] = info_last['volume']
                d_last_day['num_trades'] = info_last['num_trades']
                if d_last_day['open'] > 0 and d_last_day['volume'] > 0:
                    df_daily.iloc[-1] = pd.Series(d_last_day)
            if start_date is not None:
                df_daily = df_daily.loc[df_daily['date'] > start_date['date']].copy()
            db_manager.delete_from_collection(
                {'co_id': row_stock['co_id'], 'date': {'$in': df_daily['date'].to_list()}},
                'daily_tse')
            db_manager.insert_to_collection(df_daily, 'daily_tse')

    pool = ThreadPool(4)
    pool.starmap(_get_daily_data, list_to_get)
    pool.close()
    pool.join()


def calc_adj_factor():
    logger = create_logger()
    logger.info('=== adj factor ===')

    db_manager = DBManager(DB_SERVER, DB_NAME)
    df_stocks = db_manager.get_from_collection(
        {'ind_matlab_stock': {'$exists': True}, 'has_started': True, 'is_ignored': False}, 'stocks')
    df_stocks = df_stocks.sort_values(by=['co_id']).reset_index(drop=True)

    df_daily = db_manager.get_from_collection({}, 'daily_tse')

    df_prev_intraday = db_manager.get_from_collection({}, 'vwap_prev_intraday')

    pbar = tqdm(total=len(df_stocks))
    for index_stock, row_stock in df_stocks.iterrows():
        pbar.update(1)

        df_this_daily = df_daily.loc[df_daily['co_id'] == row_stock['co_id']].copy()
        df_this_daily = df_this_daily.sort_values(by='date', ascending=True)

        df_this_prev = df_prev_intraday.loc[df_prev_intraday['co_id'] == row_stock['co_id']].copy()
        df_this_daily = pd.merge(df_this_prev, df_this_daily, on=['co_id', 'date'], how='right')

        # this is for changing iro when the vwap_prev stock is 100
        df_this_daily['tse_id'] = df_this_daily['tse_id'].fillna(method='ffill')
        df_this_daily.loc[df_this_daily['tse_id'] != df_this_daily['tse_id'].shift(1), 'vwap_prev'] = np.nan
        df_this_daily.loc[df_this_daily['tse_id'] != df_this_daily['tse_id'].shift(1), 'vwap_prev_intraday'] = np.nan

        df_this_daily.loc[pd.isna(df_this_daily['close']), 'vwap_prev_intraday'] = np.nan
        df_this_daily['vwap_prev'] = df_this_daily['vwap_prev'].shift(-1)
        df_this_daily['vwap_prev_intraday'] = df_this_daily['vwap_prev_intraday'].shift(-1)
        try:
            df_this_daily['vwap_prev_result'] = df_this_daily.apply(
                lambda x: x['vwap_prev'] if (pd.isna(x['vwap_prev_intraday']) or x['vwap_prev_intraday'] == 0) else x[
                    'vwap_prev_intraday'], axis=1)
        except Exception:
            print()

        df_this_daily['adj_factor'] = df_this_daily['vwap_prev_result'] / df_this_daily['vwap']
        df_this_daily['adj_factor'] = df_this_daily['adj_factor'].fillna(1)
        df_this_daily.loc[df_this_daily['adj_factor'] > 1, 'adj_factor'] = 1
        df_this_daily['adj_factor'] = df_this_daily.iloc[::-1]['adj_factor'].cumprod()

        df_this_daily.loc[pd.isna(df_this_daily['vwap']), 'adj_factor'] = np.nan

        df_this_daily = df_this_daily[
            ['co_id', 'symbol', 'tse_id', 'date', 'fa_date', 'open', 'high', 'low', 'close', 'volume', 'value',
             'num_trades', 'vwap', 'adj_factor']].copy()

        db_manager.delete_from_collection({'co_id': row_stock['co_id']}, 'daily_tse')
        db_manager.insert_to_collection(df_this_daily, 'daily_tse', correct_encode=True)
