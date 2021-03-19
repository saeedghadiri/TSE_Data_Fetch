from Utils.Utils import Connection
import numpy as np
import pandas as pd
import time
import re
from Utils.Utils import replaceText,create_logger

prefixes = ['www', 'www', 'www', 'members', 'cdn', 'cdn2', 'cdn3', 'cdn4', 'cdn5', 'cdn6']

url_history_daily = "http://{}.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={}&Top=999999&A=0"


def get_random_prefix():
    return prefixes[np.random.randint(len(prefixes))]


class TSEConnection:
    def __init__(self):
        self.con = Connection()
        self.logger = create_logger()

    def get_history_daily(self, tse_id, num_stack=0):
        url = url_history_daily.format('www', tse_id)
        if num_stack == 8:
            self.logger.warning(tse_id + 'no data for history daily')
            return None
        retry_get = False
        data_daily = []
        try:
            data = self.con.get(url, timeout=10)
            data_daily = data.split(';')
            data_daily = [d.split('@') for d in data_daily]
        except Exception:
            retry_get = True

        if len(data_daily) > 1:
            df = pd.DataFrame(data_daily)
            if len(df.columns) != 10:
                retry_get = True
        else:
            retry_get = True

        if retry_get:
            df = self.get_history_daily(tse_id, num_stack + 1)

        return df

