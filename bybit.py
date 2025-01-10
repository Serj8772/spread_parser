import requests
from db_sender import *

EXCHANGE_NAME = 'bybit'
url = 'https://api.bybit.com/v5/market/tickers?category=spot'
params = {}


def bybit(url, params):
    response = requests.get(url, params=params).json()
    data_list = {}
    for pair in response['result']['list']:
        data_list[pair['symbol']] = [pair['bid1Price'], pair['ask1Price']]
    return data_list


data = bybit(url, params)
main_db(EXCHANGE_NAME, data)
