import logging
from datetime import datetime
import calendar
import pytz
import time
from kiteconnect import KiteConnect
import pandas as pd


def net_quant_zero(existing_positions,name):
    if len(existing_positions)==0 :
        return True
    else:
        p = True
        for i in existing_positions:
                if name in i['tradingsymbol'] and i['quantity'] != 0:
                    p = False
        return p

def short_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite):
    print('Scanning Entry Short Straddle Option Chain for:',name,":",len(instruments))
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
                    ltp_data = kite.ltp('NFO:'+j['tradingsymbol'])
                    if ltp_data:
                        price = ltp_data['NFO:'+j['tradingsymbol']]['last_price']
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

def long_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite):
    print('Scanning Entry Long Straddle Option Chain for:',name)
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
        if j['name'] == name and j['tradingsymbol']!='HINDCOPPER23AUG173PE':
            if j['expiry'] == last_thursday_date_dt:
                if j['instrument_type']=='PE':
                    ltp_data = kite.ltp('NFO:'+j['tradingsymbol'])
                    if ltp_data:
                        price = ltp_data['NFO:'+j['tradingsymbol']]['last_price']
                        print(j['tradingsymbol'],j['last_price'],price)
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

def get_instru_tradesymbol_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if name in position['name'] and 'PE' in position['name']:
            return position['instrument_token'],position['tradingsymbol']
        
def get_sell_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if (name in position['name']  and 'PE' in position['name']):
            return position['sell_price']
  
def cal_dates():
    IST = pytz.timezone('Asia/Kolkata')
    # Calculate the dae of last friday and thursday of the current month
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

def short_straddle(client,name,val,kite,instruments,existing_positions):
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
        if net_quant_zero(existing_positions,name):
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = short_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                print(f'\nENTERING SHORT STRADDLE FOR {val} lots\n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} \nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe}')
                # place_order(kite,tradingsymbol_ce, 0, lot_size_ce*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                # KiteConnect.ORDER_TYPE_MARKET)
                # place_order(kite,tradingsymbol_pe, 0, lot_size_pe*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)

    # Check if it's time to exit the trade
    if datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time():
        for position in existing_positions:
            if get_name_from_instrument_token(instruments,position['instrument_token']) == name and position['quantity'] < 0 and 'CE' in position['tradingsymbol']:  # Assuming short positions
                instru_ce = position['instrument_token']
                instru_pe,trad_pe = get_instru_tradesymbol_pe_from_ce(existing_positions,name)
                sell_ce = position['sell_price']
                sell_pe = get_sell_pe_from_ce(existing_positions,name)
                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                if (
                    (ltp_ce >= 2 * ltp_pe) or (ltp_pe >= 2 * ltp_ce)
                or (
                    int(datetime.now(IST).today().strftime('%d')) == last_friday
                    and datetime.now(IST).time() >= datetime.strptime('14:00', '%H:%M').time()
                )
                
                or
                (
                    (ltp_ce <= sell_ce*0.5) or (ltp_pe <= sell_pe*0.5)
                )
                ):
                    print(f'\nCode to Exit the Trade {name} ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                    # place_order(kite,position['tradingsymbol'], 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)
                    # place_order(kite,trad_pe, 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)
                else:
                    print(f'\n Exit Condtion not met for {name}, ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                break

def check_rentry_long_straddle(existing_positions,name,client):
    data = pd.read_excel('login.xlsx')
    for index, row in data.iterrows():
            if row['name']==client:
                status = eval(row['Long Straddle Status'])
                if name in status.keys():
                    if status[name]==1:
                        return False
    if len(existing_positions)==0 :
        return True
    else:
        p = True
        for i in existing_positions:
            if name in i['tradingsymbol']:
                p = False
                for index, row in data.iterrows():
                    if row['name']==client:
                        status = eval(row['Long Straddle Status'])
                        status[name] = 1
                        data.loc[index, 'Long Straddle Status'] = str(status)
                        data.DataFrame.to_excel('login.xlsx')
        return p

def long_straddle(client,name,val,kite,instruments,existing_positions):
    IST = pytz.timezone('Asia/Kolkata')
    first_friday,last_friday,last_thursday_date_dt = cal_dates()
    second_last_thursday = last_friday-8
    # Check if it's time to enter the trade
    if (
        datetime.now(IST).time() >= datetime.strptime('15:25', '%H:%M').time()
        and (
            (int(datetime.now(IST).today().strftime('%d')) >= int(first_friday) and int(datetime.now(IST).today().strftime('%d')) <= second_last_thursday)
        )
        ):
        if check_rentry_long_straddle(existing_positions,name,client):
            if net_quant_zero(existing_positions,name):
                tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = long_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite)
                if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                    print(f'\nENTERING Long STRADDLE FOR {val} lots\n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} \nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe}')
                    # place_order(kite,tradingsymbol_ce, 0, lot_size_ce*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    # KiteConnect.ORDER_TYPE_MARKET)
                    # place_order(kite,tradingsymbol_pe, 0, lot_size_pe*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)

    # Check if it's time to exit the trade
    if datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time():
        for position in existing_positions:
            if get_name_from_instrument_token(instruments,position['instrument_token']) == name and position['quantity'] < 0 and 'CE' in position['tradingsymbol']:  # Assuming short positions
                instru_ce = position['instrument_token']
                instru_pe,trad_pe = get_instru_tradesymbol_pe_from_ce(existing_positions,name)
                sell_ce = position['sell_price']
                sell_pe = get_sell_pe_from_ce(existing_positions,name)
                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                print(ltp_ce,ltp_pe)
                if (
                    (ltp_pe <= 0.65*sell_pe and ltp_ce <= 0.65*sell_ce)
                    or
                    ltp_pe >= 3*sell_pe
                    or
                    ltp_ce >= 3*sell_ce
                ):
                    print(f'\nCode to Exit the Trade {name} ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                    # place_order(kite,position['tradingsymbol'], 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)
                    # place_order(kite,trad_pe, 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)
                else:
                    print(f'\n Exit Condtion not met for {name}, ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
            break