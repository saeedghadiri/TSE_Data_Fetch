## TSE_DATA_FETCH
this project aims to download TSE data clean and add it to local DB
It uses multi processing module to faster proccessing and IO

## how to use
run main.py and it will fetch all the data to local db.


## how to handle holiday changes
Because of holiday changes in Iran (Ghamari Holidays) you should update holiday_dates collection once in a while.

for this purpose, first install node js and
https://hmarzban.github.io/pipe2time.ir/
and then set UPDATE_SHAMSI_HOLIDAYS to True