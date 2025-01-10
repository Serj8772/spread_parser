# DATABASE SETTINGS
host = ''
user = ''
password = ''
database = ''

# размер спреда для вывода
min_spread = 0.5 # установим минимальный спред
max_spread = 100 # установим максимальный спред




def links_for_pairs(cex, pair_for_link):
    if cex == 'binance':
        return f'https://www.binance.com/en/trade/{pair_for_link.replace("USD", "_USD")}'
    elif cex == 'gateio':
        return f'https://www.gate.io/trade/{pair_for_link.replace("USD", "_USD")}'
    elif cex == 'bybit':
        return f'https://www.bybit.com/en/trade/spot/{pair_for_link.replace("USD", "/USD")}'
    else:
        print('Ошибка названия биржи')

