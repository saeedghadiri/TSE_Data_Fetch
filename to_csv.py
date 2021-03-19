from Utils.DBManager import DBManager
from config import DB_SERVER, DB_NAME

symbol = 'فولاد'

db_manager = DBManager(DB_SERVER, DB_NAME)
df = db_manager.get_from_collection({'symbol': symbol}, 'daily_tse')
df = df.sort_values('date').reset_index(drop=True)
df.to_csv(symbol + '.csv')
