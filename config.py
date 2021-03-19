import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is Project Root
DB_SERVER = '127.0.0.1'
DB_NAME = 'TSE'

NO_LAST_DAYS_TO_UPDATE = 0  # number of last days to update daily data. if 0 it means update all

UPDATE_SHAMSI_HOLIDAYS = False
