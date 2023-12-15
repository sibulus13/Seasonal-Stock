import pandas as pd
from datetime import datetime
from math import ceil

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

def create_monthly_variables(dataset, sym):
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

    df['avg gain/loss ratio'] = df['avg gain']+df['avg loss']
    df['min gain/loss ratio'] = df['min gain']+df['max loss']
    df['max gain/loss ratio'] = df['max gain']+df['min loss']
        
    # save df
    save_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{sym}\data\{sym}_output.csv'
    df.to_csv(save_path)

def create_monthly_dataset(sym):
    dataset_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{sym}\data\{sym}.csv'
    # import dataset
    dataset = pd.read_csv(dataset_path)
    # remove unnamed:0
    dataset = dataset.drop(['Unnamed: 0'], axis=1)
    cols_to_add = ['week', 'month', 'year', 'month and week', 'quarter', 'max loss', 'max gain']
    # add cols
    for col in cols_to_add:
        dataset[col] = -1
    dataset = calculate_cols(dataset)
    # save dataset
    save_path = rf'D:\repo\Stock\Seasonal-Stock\dataset\{sym}\data\{sym}_data.csv'
    dataset.to_csv(save_path)

    create_monthly_variables(dataset, sym)
    
if __name__ == '__main__':
    sym = 'CVE'
    # format data_path using sym
    
    create_monthly_dataset(sym)