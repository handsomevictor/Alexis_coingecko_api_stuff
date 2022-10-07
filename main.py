from pycoingecko import CoinGeckoAPI
import time
import pandas as pd
import os
from tqdm import tqdm
import requests
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat

import datetime
import warnings

warnings.filterwarnings('ignore')


def get_alexis_data(crypto, fiat, days):
    cg = CoinGeckoAPI()
    data = cg.get_coin_market_chart_by_id(id=crypto, vs_currency=fiat, days=days)

    mkr_cap_df = data['market_caps']
    mkr_cap_df = pd.DataFrame(mkr_cap_df, columns=['unixtime', 'market_cap'])

    price_df = data['prices']
    price_df = pd.DataFrame(price_df, columns=['unixtime', 'price'])

    timestamps_list = pd.to_datetime(mkr_cap_df.unixtime, unit='ms')
    mkr_cap_df['timestamp'] = timestamps_list
    price_df['timestamp'] = timestamps_list

    # concat
    df = pd.concat([mkr_cap_df, price_df], axis=1)

    df['supply'] = df['market_cap'] / df['price']

    df.columns = ['unixtime', 'market_cap', 'timestamp', 'unixtime', 'price', 'timestamp2', 'supply']
    df = df.drop(['unixtime', 'timestamp2'], axis=1)
    df.set_index('timestamp', inplace=True)
    return df


def combine(cryptos, fiats, days_list, save_dir=None):
    for crypto, fiat, day in tqdm(zip(cryptos, fiats, days_list), total=len(cryptos)):
        df = get_alexis_data(crypto, fiat, day)
        if save_dir is not None:
            df.to_csv(os.path.join(save_dir, f'{crypto}_{fiat}_{day}.csv'))


if __name__ == '__main__':
    csv = pd.read_csv(os.path.join(os.getcwd(), 'ticker_cg.csv'))

    url = 'https://api.coingecko.com/api/v3/coins/list'
    response = requests.get(url)
    content = response.json()
    df = pd.DataFrame(content)
    df.drop(columns=['name'])

    # we create a list of strings to run our loop and an empty dataframe in which we will concatenate our results
    ids_list = list()
    list_ticker = csv.values.tolist()

    # noinspection PyTypeChecker
    list_ticker = [item for sublist in list_ticker for item in sublist]

    res = []
    for ticker in list_ticker:
        ticker_index = df[df['symbol'] == ticker].id.tolist()
        res.extend(ticker_index)

    print(res)

    cryptos = res
    fiats = ['usd' for i in range(len(cryptos))]
    days_list = [10000 for i in range(len(cryptos))]

    save_dir = os.path.join(os.getcwd(), 'data_download')
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    combine(cryptos, fiats, days_list, save_dir=save_dir)
