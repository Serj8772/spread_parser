import requests
from db_sender import *

EXCHANGE_NAME = 'gateio'
url = 'https://api.gateio.ws/api/v4/spot/tickers/'
params = {}


def gateio(url, params):
    response = requests.get(url, params=params).json()
    data_list = {}
    for pair in response:

        pair['currency_pair'] = pair['currency_pair'].replace('_', '')
        if 'USDT' in pair['currency_pair'] or 'USDC' in pair['currency_pair']:
            data_list[pair['currency_pair']] = [pair['lowest_ask'], pair['highest_bid']]
    return data_list


data_list = gateio(url, params)
# print(data_list, len(data_list))
main_db(EXCHANGE_NAME, data_list)

