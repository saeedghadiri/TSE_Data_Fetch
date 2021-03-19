import subprocess
import os
import jdatetime
import json
import pandas as pd
from datetime import datetime
from Utils.DBManager import DBManager
from Utils.Utils import to_shamsi, create_logger
from config import DB_SERVER, ROOT_DIR


def _run_pipe2time(years_to_update=3):
    path_pipe2time = os.path.join(ROOT_DIR, 'pipe2time.ir', 'pipe2time.ir-master')
    for ii in range(years_to_update):
        year = str(jdatetime.datetime.now().year + ii)
        str_call = 'npm start --prefix ' + path_pipe2time.__str__() + ' ' + year

        subprocess.call(str_call, shell=True)


def _json_to_db(db_name, db_host=DB_SERVER, years_to_update=3):
    df = pd.DataFrame(columns=['date', 'fa_date', 'reason'])
    for ii in range(years_to_update):
        year = str(jdatetime.datetime.now().year + ii)
        path_index = os.path.join(ROOT_DIR, 'pipe2time.ir', 'pipe2time.ir-master', 'api', year, 'index.json')

        with open(path_index, encoding='utf-8') as handle:
            json_dict = json.loads(handle.read())

        for d_month in json_dict[year]:
            for event in d_month['events']:
                if event['isHoliday']:
                    df.loc[len(df)] = [datetime.strptime(event['mDate'], '%Y/%m/%d'),
                                       int(jdatetime.datetime.strptime(event['jDate'], '%Y/%m/%d').strftime('%Y%m%d')),
                                       event['text']]
    db_manager = DBManager(db_host, db_name)
    db_manager.drop_collection('dates_holiday')
    db_manager.insert_to_collection(df, 'dates_holiday')


def update_holiday_dates(db_name):
    # _run_pipe2time()
    _json_to_db(db_name)


def update_dates(db_name, db_host=DB_SERVER):
    logger = create_logger()
    db_manager = DBManager(db_host, db_name)
    df_holidays = db_manager.get_from_collection({}, 'dates_holiday')
    last_day = db_manager.get_from_collection({}, 'dates', {'by': 'date', 'direction': -1}, 1)
    dates_till_today = pd.date_range(last_day['date'].values[0], datetime.today(), normalize=True,
                                     closed='right').to_pydatetime().tolist()
    df_dates_to_add = pd.DataFrame(columns=['date', 'fa_date'])
    list_holidays = df_holidays['date'].dt.to_pydatetime().tolist()
    for d_date in dates_till_today:
        if (d_date not in list_holidays) and (d_date.weekday() != 3) and (d_date.weekday() != 4):
            df_dates_to_add.loc[len(df_dates_to_add)] = [d_date, int(to_shamsi(d_date))]
    if len(df_dates_to_add) > 0:
        df_dates_to_add['ind_matlab_date'] = df_dates_to_add.index.values + last_day['ind_matlab_date'].values[0] + 1
        db_manager.insert_to_collection(df_dates_to_add, 'dates')
        logger.info(df_dates_to_add['fa_date'].values.__str__() + 'had been added to dates')
