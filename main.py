from config import DB_SERVER, DB_NAME, NO_LAST_DAYS_TO_UPDATE, UPDATE_SHAMSI_HOLIDAYS
from Utils.HandleDates import update_dates
from Utils.DBManager import DBManager
from TSE.dataFetch import get_history_daily_data, calc_adj_factor
import Utils.HandleDates

if UPDATE_SHAMSI_HOLIDAYS:
    Utils.HandleDates._run_pipe2time()
    Utils.HandleDates._json_to_db(DB_NAME, DB_SERVER)

db_manager = DBManager(DB_SERVER, DB_NAME)
update_dates(DB_NAME)
df_dates = db_manager.get_from_collection({}, 'dates')

if NO_LAST_DAYS_TO_UPDATE == 0:
    get_history_daily_data()
else:
    get_history_daily_data(df_dates.iloc[-NO_LAST_DAYS_TO_UPDATE])
calc_adj_factor()
print('data fetch finished successfully')
