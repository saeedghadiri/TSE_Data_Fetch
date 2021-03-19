from pymongo import MongoClient
from datetime import datetime
from pytz import timezone
import pandas as pd
import numpy as np


class DBManager:
    def __init__(self, host_address, database):
        self.client = MongoClient(host_address)
        self.database = database
        time_zone = timezone('Asia/Tehran')
        dt = datetime.now(time_zone)
        now = datetime(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour, minute=dt.minute,
                       second=dt.second)
        self.now = now

    def insert_to_collection(self, df, collection_name, correct_encode=False, remove_none=False):
        col = self.client[self.database][collection_name]
        if type(df) == list:
            data = df
            data = [dict(d, update_time=self.now) for d in data]
            if remove_none:
                data = [{k: v for k, v in d.items() if pd.notnull(v)} for d in data]
            if correct_encode:
                data = [correct_encoding(d) for d in data]
            col.insert_many(data)
        elif type(df) == dict:
            df['update_time'] = self.now
            data = df
            if remove_none:
                data = {k: v for k, v in data.items() if pd.notnull(v)}
            if correct_encode:
                data = correct_encoding(data)
            col.insert_one(data)
        else:
            df['update_time'] = self.now
            data = df.to_dict(orient='records')

            if correct_encode:
                data = [correct_encoding(m) for m in data]
            if remove_none:
                data = [{k: v for k, v in m.items() if pd.notnull(v)} for m in data]

            col.insert_many(data)

    def get_from_collection(self, d_filter, collection_name, sort=None, limit=None, d_select=None, return_list=False):
        col = self.client[self.database][collection_name]
        if sort is None and limit is None:
            cursor = col.find(d_filter, d_select)
        elif limit is None:
            cursor = col.find(d_filter, d_select).sort(sort['by'], sort['direction'])
        elif sort is None:
            cursor = col.find(d_filter, d_select).limit(limit)
        else:
            cursor = col.find(d_filter, d_select).sort(sort['by'], sort['direction']).limit(limit)
        if return_list:
            return list(cursor)
        else:
            df = pd.DataFrame(list(cursor))
        return df

    def agg_collection(self, d_pipeline, collection_name):
        cursor = self.client[self.database][collection_name].aggregate([d_pipeline])
        df = list(cursor)
        df = [aa['_id'] for aa in df]
        df = pd.DataFrame(df)
        return df

    def delete_from_collection(self, d_filter, collection_name):
        col = self.client[self.database][collection_name]
        col.remove(d_filter)

    def drop_collection(self, collection_name):
        self.client[self.database].drop_collection(collection_name)


def correct_encoding(inp):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inps
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    if isinstance(inp, dict):
        new = {}
        for key1, val1 in inp.items():
            new[key1] = correct_encoding(val1)

    elif isinstance(inp, list):
        new = [correct_encoding(item) for item in inp]

    elif isinstance(inp, np.bool_):
        new = bool(inp)

    elif isinstance(inp, np.int64):
        new = int(inp)

    elif isinstance(inp, np.float64):
        new = float(inp)

    elif pd.isna(inp):
        new = None

    else:
        new = inp

    return new


def transfer_db(db_source, db_dest, name_col):
    print(name_col)
    df = db_source.get_from_collection({}, name_col)
    del df['_id']
    del df['update_time']
    db_dest.drop_collection(name_col)
    db_dest.insert_to_collection(df, name_col)
