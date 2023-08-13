import sqlite3
import time
import calendar
import pytz
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from kiteconnect import KiteConnect
from datetime import datetime
from defs2 import net_quant_zero,short_get_symbol_lotsize,long_get_symbol_lotsize,place_order,get_name_from_instrument_token,get_instru_tradesymbol_pe_from_ce,cal_dates,short_straddle,long_straddle,check_rentry_long_straddle


login = pd.read_excel('login.xlsx')
IST = pytz.timezone('Asia/Kolkata')
# Generate a unique log file name with a timestamp
log_file = f"log_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# %%
# Checking connection
try:
    sqliteConnection = sqlite3.connect('SQLite_Python.db')
    cursor = sqliteConnection.cursor()
    # print("Database created and Successfully Connected to SQLite")

    sqlite_select_Query = "select sqlite_version();"
    cursor.execute(sqlite_select_Query)
    record = cursor.fetchall()
    # print("SQLite Database Version is: ", record)
    cursor.close()

except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        # print("The SQLite connection is closed")

# %%
# Creating table

try:
    # Connect to the SQLite database or create it if not exists
    sqliteConnection = sqlite3.connect('SQLite_Python.db')

    # Create a cursor to interact with the database
    cursor = sqliteConnection.cursor()
    # print("Database created and Successfully Connected to SQLite")

    # 1. Create a table named 'portfolio' with columns: name, lot_size, atm, timestamp
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS portfolio (
            tradingsymbol TEXT,
            quantity INTEGER,
            instrument_token TEXT,
            sell_price INTEGER,
            timestamp DATETIME
        );
    '''
    cursor.execute(create_table_query)
    # print("Table 'portfolio' created successfully")
    # Close the cursor
    cursor.close()
except sqlite3.Error as error:
    print("Error while working with SQLite:", error)
finally:
    # Close the database connection if it's open
    if sqliteConnection:
        sqliteConnection.close()
        # print("The SQLite connection is closed")

# %%
def cal_last_thru():
    IST = pytz.timezone('Asia/Kolkata')
    # Calculate the dae of last friday and thursday of the current month
    year = int(datetime.now(IST).today().strftime('%Y'))
    month = int(datetime.now(IST).today().strftime('%m'))
    last_day = calendar.monthrange(year, month)[1]
    last_weekday = calendar.weekday(year, month, last_day)
    last_thursday = last_day - ((7 - (3 - last_weekday)) % 7)
    last_thursday_date = datetime(year, month, last_thursday).strftime('%d-%m-%Y')
    last_thursday_date_dt = datetime.strptime(last_thursday_date,'%d-%m-%Y').date()
    return last_thursday_date_dt

print('Starting Short Straddle Bot')
session = {}
short_stock_and_quan = {}
long_stock_and_quan = {}
usr_instrums = {}


for index, row in login.iterrows():
    api_key = row['apikey']
    api_secret = row['apisecret']
    short_stock_and_quan[row['name']] = eval(row['short straddle'])
    long_stock_and_quan[row['name']] = eval(row['long straddle'])
    kite = KiteConnect(api_key=api_key)
    print('Please Login and Access your Request Token for',row['name'],kite.login_url())
    request_token = input('Please Enter the Request Token :')
    data = kite.generate_session(request_token,api_secret=api_secret)
    session[row['name']]=kite
    short_instrums = []
    for i in kite.instruments():
        if (i['name'] in eval(row['short straddle']).keys() or i['name'] in eval(row['long straddle']).keys()):
            if i['expiry'] == cal_last_thru():
                short_instrums.append(i)
    usr_instrums[row['name']] = short_instrums

# Assuming you have imported symbols and defined the short_straddle function

def check_open_order(kite,name):
    order = kite.orders()
    if len(order)==0:
        return True
    else:
        p = True
        for i in kite.orders()['data']:
            if name in i['tradingsymbol']:
                if i['status']=='OPEN':
                    p = False
        return p

def process_row(row):
    kite = session[row['name']]
    existing_positions = kite.positions()['net']
    usr_posi = []
    for i in existing_positions:
        if i['exchange']=='NFO':
            usr_posi.append(i)
    instruments = usr_instrums[row['name']]
    for key, val in short_stock_and_quan[row['name']].items():
        if check_open_order(kite,key):
            short_straddle(row['name'],key, val, kite, instruments, usr_posi)
    for key, val in long_stock_and_quan[row['name']].items():
        if check_open_order(kite,key):
            long_straddle(row['name'],key, val, kite, instruments, usr_posi)
            

# Use a ThreadPoolExecutor for managing concurrent processing
with ThreadPoolExecutor(max_workers=4) as executor:
    while True:
        if datetime.now(IST).time() >= datetime.strptime('05:30', '%H:%M').time():
            # Process each row concurrently
            futures = [executor.submit(process_row, row) for index, row in login.iterrows()]
            # Wait for all tasks to complete
            for future in futures:
                future.result()

            time.sleep(5)  # Adjust the delay as needed
            logging.info(datetime.now(IST))
        else:
            print('Session Ended. Please Restart')
            break  # Exit the loop when outside the processing time window

# %