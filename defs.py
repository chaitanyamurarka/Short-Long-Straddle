import logging
from datetime import datetime
import calendar
import sqlite3

def net_quant_zero(kite,name):
    # if len(kite.positions()['net'])==0 :
    #     return True
    # else:
    #     for i in kite.positions()['net']:
    #             if name in i['tradingsymbol'] and i['quantity']==0:
    #                 return True
    # Fetching all entries from table

    try:
        # Connect to the SQLite database or create it if not exists
        sqliteConnection = sqlite3.connect('SQLite_Python.db')

        # Create a cursor to interact with the database
        cursor = sqliteConnection.cursor()
        # print("Database created and Successfully Connected to SQLite")

        # Fetch all rows from the 'portfolio' table
        fetch_all_query = '''
            SELECT * FROM portfolio;
        '''
        cursor.execute(fetch_all_query)
        rows = cursor.fetchall()

        # Print fetched rows
        # print("Fetched Rows from 'portfolio' table:")
        if len(rows)==0:
            # Close the cursor
            cursor.close()
            return True
        elif len(rows)>0:
            p = True
            for row in rows:
                if name in str(row[0]):
                    if row[1] != 0:
                        cursor.close()
                        p = False
            return p
                
        # Close the cursor
        cursor.close()

    except sqlite3.Error as error:
        print("Error while working with SQLite:", error)
    finally:
        # Close the database connection if it's open
        if sqliteConnection:
            sqliteConnection.close()
            # print("The SQLite connection is closed")

def get_symbol_lotsize(i,name,last_thursday_date_dt,strike):
    tradingsymbol_ce=None
    lot_size_ce=None
    tradingsymbol_pe=None
    lot_size_pe = None
    # i = kite.instruments()
    for j in i:
        if j['name'] == name and j['expiry'] == last_thursday_date_dt and j['strike'] == strike and j['instrument_type']=='CE': 
            tradingsymbol_ce = j['tradingsymbol']
            lot_size_ce = j['lot_size']
            instru_ce = j['instrument_token']
        if j['name'] == name and j['expiry'] == last_thursday_date_dt and j['strike'] == strike and j['instrument_type']=='PE': 
            tradingsymbol_pe = j['tradingsymbol']
            lot_size_pe = j['lot_size']   
            instru_pe = j['instrument_token']
    return tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe,instru_ce,instru_pe

def place_order(kite,tradingSymbol, price, qty, direction, exchangeType, product, orderType):
    try:
        orderId = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchangeType,
            tradingsymbol=tradingSymbol,
            transaction_type=direction,
            quantity=qty,
            price=price,
            product=product,
            order_type=orderType)

        logging.info('Order placed successfully, orderId = %s', orderId)
        return orderId
    except Exception as e:
        logging.info('Order placement failed: %s', e.message)

def get_expiry_date_and_strike_from_instrument_token(instruments,instrument_token):
    for instrument in instruments:
        if int(instrument['instrument_token']) == int(instrument_token):
            return instrument['expiry'],instrument['strike']
    return None

def get_name_from_instrument_token(instruments,instrument_token):
    for instrument in instruments:
        if int(instrument['instrument_token']) == int(instrument_token):
            return instrument['name']

    return None

def get_instru_tradesymbol_pe_from_ce(instruments,name,stri,exp):
    for instrument in instruments:
        if instrument['name'] == name and instrument['expiry'] == exp and instrument['strike'] == stri and instrument['instrument_type']=='PE':
            return instrument['instrument_token'],instrument['tradingsymbol']
        
def cal_dates():
    # Calculate the dae of last friday and thursday of the current month
    year = int(datetime.now().today().strftime('%Y'))
    month = int(datetime.now().today().strftime('%m'))
    last_day = calendar.monthrange(year, month)[1]
    last_weekday = calendar.weekday(year, month, last_day)
    last_thursday = last_day - ((7 - (3 - last_weekday)) % 7)
    last_thursday_date = datetime(year, month, last_thursday).strftime('%d-%m-%Y')
    last_friday = last_day - ((7 - (4 - last_weekday)) % 7)
    last_thursday_date_dt = datetime.strptime(last_thursday_date,'%d-%m-%Y').date()
    first_weekday = calendar.weekday(year,month,1)
    days_to_add = (4-first_weekday+7)%7
    first_friday = 1 + days_to_add
    return first_friday,last_friday,last_thursday_date_dt

def short_straddle(name,kite,instruments,existing_positions):
    first_friday,last_friday,last_thursday_date_dt = cal_dates()
    # Check if it's time to enter the trade
    if (
        datetime.now().time() >= datetime.strptime('09:30', '%H:%M').time()
        and (
            (int(datetime.now().today().strftime('%d')) >= int(first_friday) and int(datetime.now().today().strftime('%d')) < last_friday)
        or 
        (int(datetime.now().today().strftime('%d')) == last_friday and datetime.now().time() <= datetime.strptime('14:00', '%H:%M').time())
        )
        ):
        print(name)
        if net_quant_zero(kite,name):
            ltp = kite.ltp(f'NSE:{name}')[f'NSE:{name}']['last_price']
            atm = None  # Initialize ATM to None
            for i in instruments:
                if i['name'] == name:
                    if atm is None or abs(float(i['strike']) - ltp) < abs(float(atm - ltp)):
                        atm = i['strike']
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = get_symbol_lotsize(instruments,name,last_thursday_date_dt,atm)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                print(f'\nENTERING SHORT STRADDLE FOR \n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} \nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe}')
                # place_order(kite,tradingsymbol_ce, 0, lot_size_ce, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                # KiteConnect.ORDER_TYPE_MARKET)
                # place_order(kite,tradingsymbol_pe, 0, lot_size_pe, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)
                # 2. Insert a row of data into the 'portfolio' table

                try:
                    sqliteConnection = sqlite3.connect('SQLite_Python.db')
                    cursor = sqliteConnection.cursor()
                    # print("Database created and Successfully Connected to SQLite")
                    
                    insert_data_query = '''
                        INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,timestamp)
                        VALUES (?, ?, ?,?);
                    '''
                    data_to_insert = (tradingsymbol_ce, lot_size_ce*-1,instru_ce,datetime.now())
                    cursor.execute(insert_data_query, data_to_insert)
                    data_to_insert = (tradingsymbol_pe, lot_size_pe*-1,instru_pe,datetime.now())
                    cursor.execute(insert_data_query, data_to_insert)

                    sqliteConnection.commit()
                    print("Row of data inserted into 'portfolio' table")
                    # Close the cursor
                    cursor.close()
                except sqlite3.Error as error:
                    print("Error while working with SQLite:", error)
                finally:
                    # Close the database connection if it's open
                    if sqliteConnection:
                        sqliteConnection.close()
                        # print("The SQLite connection is closed")        
        # else:
        #     print(f"\n{name} Net Quantity Not Zero")

    # Check if it's time to exit the trade
    if datetime.now().time() >= datetime.strptime('09:25', '%H:%M').time():
        # Fetching all entries from table
        try:
            # Connect to the SQLite database or create it if not exists
            sqliteConnection = sqlite3.connect('SQLite_Python.db')

            # Create a cursor to interact with the database
            cursor = sqliteConnection.cursor()
            # print("Database created and Successfully Connected to SQLite")

            # Fetch all rows from the 'portfolio' table
            fetch_all_query = '''
                SELECT * FROM portfolio;
            '''
            cursor.execute(fetch_all_query)
            rows = cursor.fetchall()
        
            for position in rows:
                if get_name_from_instrument_token(instruments,position[2]) == name and position[1] < 0 and 'CE' in position[0]:  # Assuming short positions
                    instru_ce = position[2]
                    exp,stri = get_expiry_date_and_strike_from_instrument_token(instruments,instru_ce)
                    instru_pe,trad_pe = get_instru_tradesymbol_pe_from_ce(instruments,name,stri,exp)
                    ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                    ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                    if (
                        (ltp_ce >= 2 * ltp_pe) or (ltp_pe >= 2 * ltp_ce)
                    or (
                        datetime.now().today().strftime('%A') == 'Friday'  # Check if it's a Friday
                        and datetime.now().time() >= datetime.strptime('14:00', '%H:%M').time()
                    )):
                        print('')
                        # print(f'\nCode to Exit the Trade {name} ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                        # place_order(kite,position['tradingsymbol'], 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                        #             KiteConnect.ORDER_TYPE_MARKET)
                        # place_order(kite,trad_pe, 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                        #             KiteConnect.ORDER_TYPE_MARKET)
                    # else:
                    #     print(f'\n Exit Condtion not met for {name}, ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                    break
                
            cursor.close()

        except sqlite3.Error as error:
            print("Error while working with SQLite:", error)
        finally:
            # Close the database connection if it's open
            if sqliteConnection:
                sqliteConnection.close()
                # print("The SQLite connection is closed")
