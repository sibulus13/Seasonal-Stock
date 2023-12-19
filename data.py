from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime, timedelta
import time
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame

ALPACA_API_KEY= os.getenv('ALPACA_API_KEY')
ALPACA_S_KEY= os.getenv('ALPACA_S_KEY')
ALPACA_BASE_URL = 'https://data.alpaca.markets/v2'
DATASET_DIR = r'D:\repo\Stock\Seasonal-Stock\dataset'
api = REST(ALPACA_API_KEY, ALPACA_S_KEY, ALPACA_BASE_URL, api_version='v2')

def raw_csv_path(sym):
    return os.path.join(DATASET_DIR, sym, 'data', f'{sym}.csv')


def add_unix(df):
    df['timestamp'] = df.index
    df['unixTime'] = [time.mktime(ts.timetuple()) for ts in df['timestamp']]
    return df


def get_last_available_day(timestamp_str: str):
    '''fetches the date of the last available trading day'''
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d')
    day_before = timestamp - timedelta(days=1)
    timestamp_str = day_before.strftime('%Y-%m-%d')
    return timestamp_str


def fetch_new_data(sym,
                   start_date,
                   end_date=None,
                   save=True,
                   header=True,
                   mode='w',
                   adj='raw',
                   tf=TimeFrame.Minute):
    '''
    Grabs last recorded date from CSV and fetches new data from that date to today
    Saves all datasets and latest scaled entry with corresponding timestamp
    '''
    # parsed sym is used for file naming
    parsed_sym = sym.replace('.', '_')
    old_data = pd.DataFrame()
    print(f'fetching new data for {sym}')
    try:
        old_data = pd.read_csv(raw_csv_path(parsed_sym))
        # last_recorded_timestamp = old_data.iloc[-1]['timestamp'][:10]
        start_date = old_data.iloc[-1]['timestamp'][:10]
        start_date = get_last_available_day(start_date)
        print(f'recorded day before start date: {start_date}')

    except Exception as e:
        print(f'ERROR: {e}')
        print('no old data found')
    # get new data here
    # print('fetching new data')
    data = api.get_bars(sym,
                        tf,
                        start=start_date,
                        end=end_date,
                        adjustment=adj).df
    data = add_unix(data)
    print(data.shape)
    if data.empty:
        return
    # print('cleaning raw data')
    all_data = pd.concat([data,
                          old_data]).drop_duplicates().reset_index(drop=True)
    all_data.sort_values(by=['unixTime'], inplace=True)
    all_data = all_data[all_data.columns.drop(
        list(all_data.filter(regex='Unnamed')))]

    # save data to csv
    if save:
        path = os.path.join(DATASET_DIR, parsed_sym, 'data')
        os.makedirs(path, exist_ok=True)
        path = os.path.join(DATASET_DIR, parsed_sym, 'models')
        os.makedirs(path, exist_ok=True)
        print('saving to csv')
        all_data.to_csv(raw_csv_path(sym), mode=mode, header=header)
    print(f'done data fetching for {sym}')
    print('-----------------------------------------')
    return


if __name__ == '__main__':
    volatile_syms_20230729 = ['AURC', 'BGLC', 'UFAB', 'INVO']
    big_syms = ['AAPL', 'MSFT', 'AMZN', 'GOOG', 'FB', 'TSLA', 'NVDA', 'PYPL', 'ADBE']
    syms = ['CVE']
    for sym in syms:
        start_date = '2000-01-01'
        fetch_new_data(sym, start_date=start_date, tf=TimeFrame.Week)