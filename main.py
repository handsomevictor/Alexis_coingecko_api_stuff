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


def get_data_and_save_file_for_concurrent(crypto, fiat, days, save_dir):
    df = get_alexis_data(crypto, fiat, days)
    if save_dir is not None:
        df.to_csv(os.path.join(save_dir, f'{crypto}_{fiat}_{days_list}.csv'))
        print(f'{crypto}_{fiat}_{days_list} is saved!')


def combine(cryptos, fiats, days_list, save_dir=None):
    for crypto, fiat, day in tqdm(zip(cryptos, fiats, days_list), total=len(cryptos)):
        df = get_alexis_data(crypto, fiat, day)
        if save_dir is not None:
            df.to_csv(os.path.join(save_dir, f'{crypto}_{fiat}_{day}.csv'))
        else:
            df.to_csv(os.path.join('data_download', f'{crypto}_{fiat}_{day}.csv'))


def combine_concurrent(crypto_list, fiat_list, days, save_dir=None, max_workers=60):
    start_time = time.time()
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(get_data_and_save_file_for_concurrent,
                      crypto_list,
                      fiat_list,
                      repeat(days),
                      repeat(save_dir)))

    end_time = time.time()
    print(f'Total time: {end_time - start_time:.2f} seconds')


def days_list(start_date=None, end_date=None, N=10):
    """
    :param start_date: in datetime format
    :param end_date:
    :param N:
    :return:
    """
    if start_date and end_date:
        date_list = [(end_date - start_date).days + 1 for i in range(len(cryptos))]
    else:
        days_list = [N for i in range(len(cryptos))]

    return date_list


def read_single_result_file(id, fiat, days):
    df = pd.read_csv(os.path.join(os.getcwd(), 'data_download', f'{id}_{fiat}_{days}.csv'),
                     index_col=0).reset_index()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour

    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d')

    return df[df['hour'] == 0][['timestamp', 'market_cap', 'supply', 'price']].set_index('timestamp')


def combine_several_id(id_list, days_list, fiat_list):
    file_dir = os.path.join(os.getcwd(), 'data_download')
    target_zip = zip(id_list, days_list, fiat_list)

    res_df = pd.DataFrame()

    for id, day, fiat in target_zip:
        df = read_single_result_file(id, fiat, day)
        df['id'] = id
        res_df = pd.concat([res_df, df], axis=0)

    return res_df[['id', 'supply']]


def reformat_combined_result(id_list, days_list, fiat_list):
    df = combine_several_id(id_list, days_list, fiat_list)
    all_timestamp = sorted(df.index.unique().tolist())
    res = pd.DataFrame(index=all_timestamp)

    temp = {}
    for id in id_list:
        temp[id] = df[df['id'] == id]['supply'].rename(id)

    for id in id_list:
        res = pd.concat([res, temp[id]], axis=1)
    res = res.fillna(-1)

    sub_dir = os.path.join(os.getcwd(), 'data_download', 'combined')
    if not os.path.exists(sub_dir):
        os.makedirs(sub_dir)
    res.to_csv(os.path.join(sub_dir, 'combined_result.csv'))
    return res


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

    # ------------------------------------------------------------------------------------------------------------------
    # start downloading
    cryptos = res
    fiats = ['usd' for i in range(len(cryptos))]

    # ---------
    # for reformating and combining all results
    id_list = ['bitcoin', 'ethereum-wormhole']
    days_list = [3, 99]
    fiat_list = ['usd', 'usd']

    print(reformat_combined_result(id_list, days_list, fiat_list))

