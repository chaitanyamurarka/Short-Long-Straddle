import logging
from datetime import datetime
import calendar
import sqlite3
import pytz
import time
from kiteconnect import KiteConnect
  
IST = pytz.timezone('Asia/Kolkata')

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
            quan = 0
            for row in rows:
                if name in str(row[0]):
                    quan += row[1]
            if quan == 0:
                cursor.close()
                return True
            else:
                cursor.close()
                return False
            
        # Close the cursor
        cursor.close()

    except sqlite3.Error as error:
        print("Error while working with SQLite:", error)
    finally:
        # Close the database connection if it's open
        if sqliteConnection:
            sqliteConnection.close()
            # print("The SQLite connection is closed")

def get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite):
    IST = pytz.timezone('Asia/Kolkata')
    ltp = kite.ltp(f'NSE:{name}')[f'NSE:{name}']['last_price']
    strike = None  # Initialize ATM to None
    diff = None
    tradingsymbol_ce=None
    lot_size_ce=None
    tradingsymbol_pe=None
    lot_size_pe = None
    for i in instruments:
        if i['instrument_type']=='CE':
            if i['name'] == name:
                if i['expiry'] == last_thursday_date_dt:
                    if strike is None or abs(float(i['strike']) - ltp) < diff:
                        strike = i['strike']
                        diff = abs(float(strike - ltp))
                        tradingsymbol_ce = i['tradingsymbol']
                        lot_size_ce = i['lot_size']
                        instru_ce = i['instrument_token']
    ce_ltp = kite.ltp(f'NFO:{tradingsymbol_ce}')[f'NFO:{tradingsymbol_ce}']['last_price']
    pe_ltp = None
    diff = None
    for j in instruments:
        if j['name'] == name:
            if j['expiry'] == last_thursday_date_dt:
                if j['instrument_type']=='PE':
                    price = kite.ltp('NFO:'+j['tradingsymbol'])['NFO:'+j['tradingsymbol']]['last_price']
                    if price != 0:
                        if pe_ltp is None or abs(float(price) - ce_ltp) < diff:
                            pe_ltp = price
                            diff = abs(float(price - ce_ltp))
                            tradingsymbol_pe = j['tradingsymbol']
                            lot_size_pe = j['lot_size']   
                            instru_pe = j['instrument_token']
        time.sleep(0.3)
        logging.info(datetime.now(IST))
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


def get_name_from_instrument_token(instruments,instrument_token):
    for instrument in instruments:
        if int(instrument['instrument_token']) == int(instrument_token):
            return instrument['name']

    return None

def get_instru_tradesymbol_pe_from_ce(rows,name):
    for row in rows:
        if name in str(row[0]) and 'PE' in str(row[0]):
            return row[2],row[0]
        
def get_sell_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if (name in position[0]  and 'PE' in position[0]):
            return position[3]
        
def cal_dates():
    # Calculate the dae of last friday and thursday of the current month
    IST = pytz.timezone('Asia/Kolkata')
    year = int(datetime.now(IST).today().strftime('%Y'))
    month = int(datetime.now(IST).today().strftime('%m'))
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

def short_straddle(name,val,kite,instruments,existing_positions):
    IST = pytz.timezone('Asia/Kolkata')
    first_friday,last_friday,last_thursday_date_dt = cal_dates()
    # Check if it's time to enter the trade
    if (
        datetime.now(IST).time() >= datetime.strptime('09:30', '%H:%M').time()
        and (
            (int(datetime.now(IST).today().strftime('%d')) >= int(first_friday) and int(datetime.now(IST).today().strftime('%d')) < last_friday)
        or 
        (int(datetime.now(IST).today().strftime('%d')) == last_friday and datetime.now(IST).time() <= datetime.strptime('14:00', '%H:%M').time())
        )
        ):
        if net_quant_zero(kite,name):
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                print(f'\nENTERING SHORT STRADDLE FOR \n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} & {val} lots\nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe} & {val} lots')

                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']

                try:
                    sqliteConnection = sqlite3.connect('SQLite_Python.db')
                    cursor = sqliteConnection.cursor()
                    # print("Database created and Successfully Connected to SQLite")
                    
                    insert_data_query = '''
                        INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,sell_price,timestamp)
                        VALUES (?, ?, ?,?,?);
                    '''
                    data_to_insert = (tradingsymbol_ce, lot_size_ce*-1*val,instru_ce,ltp_ce,datetime.now(IST))
                    cursor.execute(insert_data_query, data_to_insert)
                    data_to_insert = (tradingsymbol_pe, lot_size_pe*-1*val,instru_pe,ltp_pe,datetime.now(IST))
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
    if datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time():
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
                    quan = 0
                    for lol in rows:
                        if lol[0]==position[0]:
                            quan += lol[1]
                    if quan < 0:
                        instru_ce = position[2]
                        instru_pe,trad_pe = get_instru_tradesymbol_pe_from_ce(rows,name)
                        for lol in rows:
                            if lol[0]==position[0]:
                                sell_ce = position[3]
                        sell_pe = get_sell_pe_from_ce(rows,name)
                        ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                        ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                        print(ltp_ce,ltp_pe)
                        if (
                            (ltp_ce >= 2 * ltp_pe) or (ltp_pe >= 2 * ltp_ce)
                        or (
                            int(datetime.now(IST).today().strftime('%d')) == last_friday  # Check if it's a Friday
                            and datetime.now(IST).time() >= datetime.strptime('14:00', '%H:%M').time()
                        )
                        or
                        (
                            # (ltp_ce <= sell_ce*0.5) or (ltp_pe <= sell_pe*0.5)
                            (ltp_ce <= sell_ce*0.95) or (ltp_pe <= sell_pe*0.95) or (ltp_ce >= sell_ce*1.05) or (ltp_pe >= sell_pe*1.05)
                        )):
                            try:
                                print(f'\nExiting SHORT STRADDLE FOR \n{position[0]} Of Quantity {position[1]} \nand\n{trad_pe} of Quantity {position[1]}')
                                sqliteConnection = sqlite3.connect('SQLite_Python.db')
                                cursor = sqliteConnection.cursor()
                                # print("Database created and Successfully Connected to SQLite")
                                
                                insert_data_query = '''
                                    INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,sell_price,timestamp)
                                    VALUES (?, ?, ?,?,?);
                                '''
                                data_to_insert = (position[0], position[1]*-1,instru_ce,ltp_ce,datetime.now(IST))
                                cursor.execute(insert_data_query, data_to_insert)
                                data_to_insert = (trad_pe, position[1]*-1,instru_pe,ltp_pe,datetime.now(IST))
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
                            # print(f'\nCode to Exit the Trade {name} ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                            # place_order(kite,position['tradingsymbol'], 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                            #             KiteConnect.ORDER_TYPE_MARKET)
                            # place_order(kite,trad_pe, 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                            #             KiteConnect.ORDER_TYPE_MARKET)
                        # else:
                        #     print(f'\n Exit Condtion not met for {name}, ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                        break
            if sqliteConnection:    
                cursor.close()

        except sqlite3.Error as error:
            print("Error while working with SQLite:", error)
        finally:
            # Close the database connection if it's open
            if sqliteConnection:
                sqliteConnection.close()
                # print("The SQLite connection is closed")
