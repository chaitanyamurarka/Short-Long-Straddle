# %%
from kiteconnect import KiteConnect
from datetime import datetime
import sqlite3
import time
from defs import net_quant_zero,get_symbol_lotsize,place_order,get_expiry_date_and_strike_from_instrument_token,get_name_from_instrument_token,get_instru_tradesymbol_pe_from_ce,cal_dates,short_straddle
import pandas as pd
login = pd.read_excel('login.xlsx')

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
print('Starting Short Straddle Bot')
ins = {}
for index, row in login.iterrows():
    api_key = row['apikey']
    api_secret = row['apisecret']
    symbols = eval(row['Stock'])
    kite = KiteConnect(api_key=api_key)
    print('Please Login and Access your Request Token for',row['name'],kite.login_url())
    request_token = input('Please Enter the Request Token :')
    data = kite.generate_session(request_token,api_secret=api_secret)
    access_token = data["access_token"]
    row['access_token']=access_token
    ins[row['name']]=kite

for index, row in login.iterrows():
    kite = ins[row['name']]
    instruments = kite.instruments()
    while True:
        if datetime.now().time() != datetime.strptime('05:30', '%H:%M').time():
            existing_positions = kite.positions()['net']

            # Create and start a process for each symbol
            for key,val in symbols.items():
                short_straddle(key[4:],val,kite,instruments,existing_positions)
            
            time.sleep(5)
        else:
            print('! Session Ended Pls Restart')


