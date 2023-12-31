import pandas as pd
from datetime import datetime
from math import ceil
import matplotlib.pyplot as plt
import numpy as np

# calculate week of month from timestamp date
def week_of_month(dt):
    """ Calculates the week of the month for the specified date.
    """
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()
    return int(ceil(adjusted_dom/7.0))
    
def month_of_year(dt):
    return dt.month

def quarter(dt):
    return int(ceil(dt.month/3))

def max_loss(entry1, entry2):
    # calculates the difference between entry1's high and entry2's low\
    # entry1 and entry2 are rows of the dataset
    return (entry2['low'] - entry1['high'])/entry1['close']*100
    
def max_gain(entry1, entry2):
    # calculates the difference between entry1's high and entry2's low\
    # entry1 and entry2 are rows of the dataset
    return (entry2['high'] - entry1['low'])/entry1['close']*100

def calculate_cols(dataset):
    for index, row in dataset.iterrows():
        # calculate week of month
        date = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S+00:00')
        dataset.at[index, 'week'] = week_of_month(date)
        dataset.at[index, 'month'] = month_of_year(date)
        dataset.at[index, 'quarter'] = quarter(date)
        dataset.at[index, 'year'] = date.year
        if index < len(dataset) - 1:
            dataset.at[index, 'max loss'] = max_loss(row, dataset.iloc[index+1])
            dataset.at[index, 'max gain'] = max_gain(row, dataset.iloc[index+1])
    # normalize week of month by subtracting the min week of month for that month from every entry
    dataset['week'] = dataset['week'] - dataset.groupby(['year', 'month'])['week'].transform('min') + 1
    dataset['month and week'] = dataset['month'].astype(str) + '-' + dataset['week'].astype(str)
    return dataset

def create_monthly_variables(dataset, sym, save = True, plot = True):
    month_and_weeks = dataset['month and week'].unique()
    # remove '*-5' from month and week
    month_and_weeks = [x for x in month_and_weeks if '-5' not in x]
    df = pd.DataFrame(columns=['month and week', 'max loss', 'avg loss', 'min loss', 'std loss',  'min gain', 'avg gain','max gain', 'std gain'])
    df['month and week'] = month_and_weeks
    df = df.set_index('month and week')

    # fill in the dataframe
    for month_and_week in month_and_weeks:
        # get data for that month and week
        month_and_week_data = dataset[dataset['month and week'] == month_and_week]
        # get loss and gain data
        max_loss_data = month_and_week_data['max loss']
        max_gain_data = month_and_week_data['max gain']
        # fill in the dataframe
        df.at[month_and_week, 'avg loss'] = max_loss_data.mean()
        df.at[month_and_week, 'avg gain'] = max_gain_data.mean()
        df.at[month_and_week, 'min loss'] = max_loss_data.max()
        df.at[month_and_week, 'min gain'] = max_gain_data.min()
        df.at[month_and_week, 'max loss'] = max_loss_data.min()
        df.at[month_and_week, 'max gain'] = max_gain_data.max()
        df.at[month_and_week, 'std loss'] = max_loss_data.std()
        df.at[month_and_week, 'std gain'] = max_gain_data.std()
        # fill in the month, and week, number of years
        df.at[month_and_week, 'month'] = month_and_week_data['month'].iloc[0]
        df.at[month_and_week, 'week'] = month_and_week_data['week'].iloc[0]
        df.at[month_and_week, 'num years'] = month_and_week_data['year'].nunique()

    df['avg gain/loss ratio'] = df['avg gain']+df['avg loss']
    df['min gain/loss ratio'] = df['min gain']+df['max loss']
    df['max gain/loss ratio'] = df['max gain']+df['min loss']
    df['num years'] = dataset['year'].nunique()
    print(dataset['year'].unique())
    # save df
    save_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{sym}\data\{sym}_output.csv'
    df.to_csv(save_path)
    
def plot_last_3_year_data(dataset, sym, plot = False, save = True):
    # get unique month and week values
    month_and_weeks = dataset['month and week'].unique()
    # remove '*-5' from month and week
    month_and_weeks = [x for x in month_and_weeks if '-5' not in x]
    
    # show at least 3 years of data if there are more than 3 years
    years_to_show = dataset['year'].unique()
    if len(years_to_show) > 3:
        years_to_show = years_to_show[-3:]
    # plot
    for year in years_to_show:
        # get data for that year
        year_data = dataset[dataset['year'] == year]
        # get data for those in month_and_weeks
        year_data = year_data[year_data['month and week'].isin(month_and_weeks)]
        # plot
        plt.scatter(year_data['month and week'], year_data['max loss']+year_data['max gain'], label=f'{year} margin')
    # replace x label with month and weeks and rotate
    plt.xticks(np.arange(len(month_and_weeks)), month_and_weeks)
    plt.grid(axis='x')
    # horizontal line on 0
    plt.axhline(y=0, color='r', linestyle='-')
    # Only show the first label of every month
    plt.gca().xaxis.set_major_locator(plt.MultipleLocator(4))
    plt.legend(loc='lower left')
    plt.title(f'{sym} Max Gains and Losses')
    plt.xlabel('Month and Week')
    plt.ylabel('Profit %')
    plt.xticks(rotation=90)
    if plot:
        plt.show()
    if save:
        parsed_sym = sym.replace('.', '_')
        save_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{parsed_sym}\{parsed_sym}_plot.png'
        plt.savefig(save_path)

def create_monthly_dataset(sym, plot = False, save = False):
    # parsed sym is used for file naming
    parsed_sym = sym.replace('.', '_')
    dataset_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{parsed_sym}\data\{parsed_sym}.csv'
    # import dataset
    try:
        dataset = pd.read_csv(dataset_path)
    except Exception as e:
        print(f'ERROR: {e}')
        print('no old data found')
        return
    # remove unnamed:0
    dataset = dataset.drop(['Unnamed: 0'], axis=1)
    cols_to_add = ['week', 'month', 'year', 'month and week', 'quarter', 'max loss', 'max gain']
    # add cols
    for col in cols_to_add:
        dataset[col] = -1
    dataset = calculate_cols(dataset)
    # save dataset
    save_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{parsed_sym}\data\{parsed_sym}_data.csv'
    dataset.to_csv(save_path)
    create_monthly_variables(dataset, sym)
    plot_last_3_year_data(dataset, sym, plot, save)
    if save:
        save_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{parsed_sym}\data\{parsed_sym}_data.csv'
        dataset.to_csv(save_path)


if __name__ == '__main__':
    sym = 'CVE'
    plot = True
    save = True

    create_monthly_dataset(sym, plot=plot, save=save)