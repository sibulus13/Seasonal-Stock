from data import fetch_new_data
from analysis import create_monthly_dataset
from alpaca_trade_api.rest import TimeFrame

def main():
    pass


if __name__ == '__main__':
    syms = ['CVE']
        
    for sym in syms:
        start_date = '2000-01-01'
        fetch_new_data(sym, start_date=start_date, tf=TimeFrame.Week)
        create_monthly_dataset(sym)