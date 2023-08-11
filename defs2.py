import logging
from datetime import datetime
import calendar

def net_quant_zero(existing_positions,name):
    if len(existing_positions)==0 :
        return True
    else:
        p = True
        for i in existing_positions:
                if name in i['tradingsymbol'] and i['quantity'] != 0:
                    p = False
        return p

def get_symbol_lotsize(i,name,last_thursday_date_dt,strike):
    tradingsymbol_ce=None
    lot_size_ce=None
    tradingsymbol_pe=None
    lot_size_pe = None
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
        
def get_sell_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if (name in position['name']  and 'PE' in position['name']):
            return position['sell_price']
  
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

def short_straddle(name,val,kite,instruments,existing_positions):
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
        if net_quant_zero(existing_positions,name):
            ltp = kite.ltp(f'NSE:{name}')[f'NSE:{name}']['last_price']
            atm = None  # Initialize ATM to None
            diff = None
            for i in instruments:
                if i['name'] == name:
                    if atm is None or abs(float(i['strike']) - ltp) < diff:
                        atm = i['strike']
                        diff = abs(float(atm - ltp))
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = get_symbol_lotsize(instruments,name,last_thursday_date_dt,atm)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                print(f'\nENTERING SHORT STRADDLE FOR {val} lots\n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} \nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe}')
                # place_order(kite,tradingsymbol_ce, 0, lot_size_ce*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                # KiteConnect.ORDER_TYPE_MARKET)
                # place_order(kite,tradingsymbol_pe, 0, lot_size_pe*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)

    # Check if it's time to exit the trade
    if datetime.now().time() >= datetime.strptime('09:25', '%H:%M').time():
        for position in existing_positions:
            if get_name_from_instrument_token(instruments,position['instrument_token']) == name and position['quantity'] < 0 and 'CE' in position['tradingsymbol']:  # Assuming short positions
                instru_ce = position[2]
                exp,stri = get_expiry_date_and_strike_from_instrument_token(instruments,instru_ce)
                instru_pe,trad_pe = get_instru_tradesymbol_pe_from_ce(instruments,name,stri,exp)
                sell_ce = position['sell_price']
                sell_pe = get_sell_pe_from_ce(existing_positions,name)
                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                if (
                    (ltp_ce >= 2 * ltp_pe) or (ltp_pe >= 2 * ltp_ce)
                or (
                    int(datetime.now().today().strftime('%d')) == last_friday
                    and datetime.now().time() >= datetime.strptime('14:00', '%H:%M').time()
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
