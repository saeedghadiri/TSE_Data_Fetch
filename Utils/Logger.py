import os
from datetime import timezone
import datetime
from pytz import timezone
import json
from config import ROOT_DIR


class Logger:
    def __init__(self):
        cwd = os.getcwd()
        self.timeZone = timezone('Asia/Tehran')

        dt = datetime.datetime.now(self.timeZone)
        self.folder_path = os.path.join(ROOT_DIR, 'logs', dt.strftime('%Y%m%d'))
        try:
            os.makedirs(os.path.dirname(self.folder_path))
        except Exception:
            pass

        self.file_normal = open()

        self.syncFileLog(
            '****************************************************** new session ******************************************************')

    def syncFileLog(self, text):
        try:
            dt = datetime.datetime.now(timezone('Asia/Tehran')).strftime('%H%M%S')
            with open(self.filePath, 'a+', encoding='utf-8') as file:
                file.write('@' + dt + ' | ' + text.__str__() + '\n')
                file.flush()
                os.fsync(file.fileno())
        except Exception as err:
            raise err
