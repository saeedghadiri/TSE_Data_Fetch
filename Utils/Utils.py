import jdatetime
from datetime import datetime
import requests
import json
import time
import numpy as np
import urllib
from config import DB_SERVER, ROOT_DIR
import os
import multiprocessing, logging


class Connection:
    def __init__(self):
        self.session = requests.Session()
        self.headers = None

    def fetch(self, full_url, data=None, method='POST', timeout=40):
        if method == 'GET':
            if data != None:
                url_values = urllib.parse.urlencode(data, encoding='utf-8')
                full_url = full_url + '?' + url_values
                # print(full_url)

            with self.session.get(full_url, headers=self.headers, timeout=timeout) as response:
                response.raise_for_status()
                try:
                    response = response.json()
                except ValueError:
                    response = response.text
                return response

        else:

            with self.session.post(full_url, timeout=timeout, headers=self.headers, data=json.dumps(data)) as response:
                response.raise_for_status()
                response = response.json()
                return response

    def get_data(self, full_url, data=None, method='POST', total_num_retry=5, wait_retry=1, timeout=40):
        data = None
        num_retry = 0
        while data is None and num_retry < total_num_retry:
            try:
                # data = self.fetch(full_url=full_url, data=data, method=method, timeout=timeout)
                response = requests.get(full_url)
                data = response.text

            except (
                    requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    requests.exceptions.HTTPError):
                num_retry = num_retry + 1
                print('timed_out num_retry:' + str(num_retry))
                wait_retry = np.max([1, wait_retry + 1.5 * np.random.randn()])
                time.sleep(wait_retry)
        return data

    def get(self, full_url, timeout, verify=False):
        response = requests.get(full_url, timeout=timeout, verify=verify)
        response.raise_for_status()
        try:
            response = response.json()
        except ValueError:
            response = response.text
        return response


def to_shamsi(d_date, input_format='%Y%m%d', output_format='%Y%m%d'):
    if d_date is None:
        return None
    if type(d_date) is int:
        d_date = datetime.strptime(str(d_date), input_format)
    elif type(d_date) is str:
        d_date = datetime.strptime(d_date, input_format)
    return jdatetime.datetime.fromgregorian(datetime=d_date).strftime(output_format)


def to_gregorian(d_date, input_format='%Y%m%d'):
    if type(d_date) is int:
        d_date = str(d_date)
    return jdatetime.datetime.strptime(d_date, input_format).togregorian()


def replaceText(word):
    replacements = {'پ': '\u067e', 'چ': '\u0686', 'ج': '\u062c', 'ح': '\u062d', 'خ': '\u062e', 'ه': '\u0647',
                    'ع': '\u0639', 'غ': '\u063a', 'ف': '\u0641', 'ق': '\u0642', 'ث': '\u062b', 'ص': '\u0635',
                    'ض': '\u0636', 'گ': '\u06af', 'ک': '\u06a9', 'م': '\u0645', 'ن': '\u0646', 'ت': '\u062a',
                    'ا': '\u0627', 'ل': '\u0644', 'ب': '\u0628', 'ي': '\u06cc', 'س': '\u0633', 'ش': '\u0634',
                    'و': '\u0648', 'ئ': '\u0626', 'د': '\u062f', 'ذ': '\u0630', 'ر': '\u0631', 'ز': '\u0632',
                    'ط': '\u0637', 'ظ': '\u0638', 'ژ': '\u0698', 'آ': '\u0622', 'ي': '\u064a', '؟': '\u061f',
                    'ك': '\u06a9', 'ي': 'ی'}
    for src, target in replacements.items():
        word = word.replace(src, target)

    return word


def find_ind_matlab_time_m1(df, market_name):
    start_trade = df.iloc[0]['date_time']
    start_time_market = start_trade.replace(hour=9, minute=0, second=0)
    if start_trade < start_time_market:
        start_time_market = start_trade
    stop_time_market = start_trade.replace(hour=12, minute=30, second=0)

    is_normal = True

    if market_name == 'صندوق طلا' or (df['date_time'] > stop_time_market).any():
        is_normal = False

    if is_normal:
        df['ind_matlab_time'] = ((df['date_time'] - start_time_market).dt.total_seconds() / 60).astype(int)
    else:
        if len(df) > 211:
            df.loc[210, 'open'] = df['open'].iloc[0]
            df.loc[210, 'high'] = df['high'].max()
            df.loc[210, 'low'] = df['low'].min()
            df.loc[210, 'close'] = df['close'].iloc[-1]
            df.loc[210, 'volume'] = df['volume'].sum()
            df.loc[210, 'value'] = df['value'].sum()
            df = df.iloc[:211, :].copy()
        ind_matlab_time = np.random.choice(np.arange(211), len(df), replace=False)
        ind_matlab_time.sort()
        df['ind_matlab_time'] = ind_matlab_time

    return df


def create_logger():
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter( \
        '[%(asctime)s| %(levelname)s| %(funcName)s()] %(message)s')
    try:
        os.mkdir(os.path.join(ROOT_DIR, 'logs'))
    except Exception:
        pass
    file_handler = logging.FileHandler(os.path.join(ROOT_DIR, 'logs', datetime.now().strftime('%Y%m%d') + '.log'))
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # this bit will make sure you won't have
    # duplicated messages in the output
    if not len(logger.handlers):
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.info('************ new session ************')
    return logger
