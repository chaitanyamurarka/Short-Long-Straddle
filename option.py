import logging
from datetime import datetime
import calendar
from expressoptionchain.option_stream import OptionStream
from expressoptionchain.helper import get_secrets
from expressoptionchain.option_chain import OptionChainFetcher
logging.basicConfig(level=logging.DEBUG)
def start_option_stream(secrets, symbols):
    year = int(datetime.now().today().strftime('%Y'))
    month = int(datetime.now().today().strftime('%m'))
    last_day = calendar.monthrange(year, month)[1]
    last_weekday = calendar.weekday(year, month, last_day)
    last_thursday = last_day - ((7 - (3 - last_weekday)) % 7)
    last_thursday_date = datetime(year, month, last_thursday).strftime('%d-%m-%Y')
    # there is no limit on the number of symbols to subscribe to
    stream = OptionStream(symbols, secrets, expiry=last_thursday_date)
    stream.start(threaded=True)
    option_chain_fetcher = OptionChainFetcher()