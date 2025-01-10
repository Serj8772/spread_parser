import requests
import psycopg2
import asyncio
import aiohttp
from db_sender import *


EXCHANGE_NAME = 'binance'


# получаем список всех торгуемых пар
url = 'https://api.binance.com/api/v3/ticker/price'
r = requests.request('GET', url)

list_of_pairs = []
for pair in r.json():
    pair_symbol = pair['symbol']

    if 'USDT' in pair['symbol'] or 'USDC' in pair['symbol']:
        list_of_pairs += [pair_symbol]

print(len(list_of_pairs))



data_list = {}

async def fetch_price(session, symbol):

    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}"
    async with session.get(url) as response:
        if response.status == 200:
            try:
                data = await response.json()

                # Проверяем, есть ли данные в asks и bids
                ask = data['asks'][0][0] if data['asks'] else None
                bid = data['bids'][0][0] if data['bids'] else None

                return symbol, ask, bid
            except Exception as e:
                print(f"Ошибка при обработке данных для {symbol}: {e}")
                return symbol, None, None
        else:
            print(f"Ошибка {response.status} при запросе {url}")
            return symbol, None, None


async def get_prices(symbols):

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        tasks = [fetch_price(session, symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
    return results

async def main():
    # Список торговых пар
    symbols = list_of_pairs
    # Получение цен
    prices = await get_prices(symbols)

    # Вывод результатов
    for symbol, ask, bid in prices:
        if ask:
            # print(f"Торговая пара: {symbol}, Bid: {ask}, Ask: {bid}")
            data_list[symbol] = [ask, bid]
        else:
            print(f"Не удалось получить цену для {symbol}")

    main_db(EXCHANGE_NAME, data_list)

# Запуск асинхронного кода
asyncio.run(main())
