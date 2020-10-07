from multiprocessing import Pool
import time
from collections import Counter
import numpy as np
import pandas as pd


def s_filter(string):
    for char in string:
        if char == '*' or char.isdigit():
            return False
        else:
            return True


def s_filter(string):
    for char in string:
        if char == '*' or char.isdigit():
            return False
        else:
            return True


def process(chunk):
    bids = [i for i in range(8, 88, 8)]
    asks = [i for i in range(12, 92, 8)]
    chunk['Bids'] = chunk.iloc[:, bids].replace(np.nan, '').apply(lambda x: ''.join(filter(s_filter, ';'.join(x))),
                                                                  axis=1)
    chunk['Asks'] = chunk.iloc[:, asks].replace(np.nan, '').apply(lambda x: ''.join(filter(s_filter, ';'.join(x))),
                                                                  axis=1)
    bid_df = chunk.groupby(['#RIC'])['Bids'].apply(lambda x: Counter(';'.join(x).split(';'))).reset_index().replace('',
        np.nan).dropna().reset_index(
        drop=True).rename(columns={'level_1': 'MarketMaker'})
    ask_df = chunk.groupby(['#RIC'])['Asks'].apply(lambda x: Counter(';'.join(x).split(';'))).reset_index().replace('',
        np.nan).dropna().reset_index(
        drop=True).rename(columns={'level_1': 'MarketMaker'})
    return [bid_df, ask_df]


result_list = []
def log_result(result):
    result_list.append(result)


if __name__ == '__main__':
    pool = Pool(12)
    t1 = time.time()
    file_path = r'D:\LiBao\data_20200904\NSQ-1\NSQ-2016-01-05-NASDAQL2-Data-1-of-1-a1.csv'
    chunks = pd.read_csv(file_path, chunksize=100000)
    for n, chunk in enumerate(chunks):
        print(n)
        pool.apply_async(process, args=(chunk, ), callback=log_result)
    pool.close()
    pool.join()
    bids_all = pd.concat([res[0] for res in result_list], ignore_index=True)
    asks_all = pd.concat([res[1] for res in result_list], ignore_index=True)
    bids_all.groupby(['#RIC', 'MarketMaker']).sum().reset_index().to_csv(r'D:\LiBao\bid_list_20160105.csv', index=False)
    asks_all.groupby(['#RIC', 'MarketMaker']).sum().reset_index().to_csv(r'D:\LiBao\ask_list_20160105.csv', index=False)
    t3 = time.time()
    print("Total time", t3 - t1)

