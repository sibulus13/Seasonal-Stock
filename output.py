import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob

# recursively read all "{}_output.csv" files in the current directory with their {names} in a separate column
# and return a pandas dataframe with all the data
def get_all_csv_data():
    # get all the csv files in the current directory
    # csv_files = glob.glob("*_output.csv")
    # get all csv files in the current directory and all subdirectories named '{}_output.csv'
    csv_files = glob.glob("**/*_output.csv", recursive=True)    
    # create a list of dataframes
    df_list = []
    # iterate over the csv files
    for csv_file in csv_files:
        # get the name of the file
        name = csv_file.split("_")[0].split("\\")[-1]
        # read the csv file into a pandas dataframe
        df = pd.read_csv(csv_file)
        # add a column with the name of the file
        df["name"] = name
        # append the dataframe to the list of dataframes
        df_list.append(df)
    # concatenate all the dataframes into a single dataframe
    df = pd.concat(df_list)
    # return the dataframe
    return df

def create_cumulative_csv(data):
    # Create a new dataframe with the unique "month and weeks" as the columns and "top performer" as row
    df = pd.DataFrame(columns=["month and week"])
    df = df.set_index("month and week")
    # add ['top performer 1', 'top performer 2', 'top performer 3'] as columns
    df['top performer 1'] = None
    df['top performer 2'] = None
    df['top performer 3'] = None
    # set top performer gain/loss ratio as additional columns for each top performer
    df['top performer 1 gain/loss ratio'] = None
    df['top performer 2 gain/loss ratio'] = None
    df['top performer 3 gain/loss ratio'] = None

    # iterate over the unique "month and weeks"
    for month_and_week in data["month and week"].unique():
        # get the data for the current "month and week"
        df_month_and_week = data[data["month and week"] == month_and_week]
        # set num years, month, week
        df.loc[month_and_week, 'num years'] = df_month_and_week.iloc[0]["num years"]
        df.loc[month_and_week, 'month'] = df_month_and_week.iloc[0]["month"]
        df.loc[month_and_week, 'week'] = df_month_and_week.iloc[0]["week"]
        # get the top 3 performers for the current "month and week"
        top_performers = df_month_and_week.sort_values(by="avg gain/loss ratio", ascending=False).head(3)
        # set the top performers in the dataframe
        df.loc[month_and_week, 'top performer 1'] = top_performers.iloc[0]["name"]
        df.loc[month_and_week, 'top performer 2'] = top_performers.iloc[1]["name"]
        df.loc[month_and_week, 'top performer 3'] = top_performers.iloc[2]["name"]
        # set the gain/loss ratio for each top performer
        df.loc[month_and_week, 'top performer 1 gain/loss ratio'] = top_performers.iloc[0]["avg gain/loss ratio"]
        df.loc[month_and_week, 'top performer 2 gain/loss ratio'] = top_performers.iloc[1]["avg gain/loss ratio"]
        df.loc[month_and_week, 'top performer 3 gain/loss ratio'] = top_performers.iloc[2]["avg gain/loss ratio"]
        
    # sort by month and then week
    df = df.sort_values(by=["month", "week"], ascending=True)
    # open a csv 
    # save the dataframe to a csv file
    df.to_csv(r"D:\repo\Stock\Seasonal-Stock\dataset\seasonal_top_performer.csv")
    return df

def create_comprehensive_seasonal_csv():
    # get all the csv data
    data = get_all_csv_data()
    df = create_cumulative_csv(data)
    
def get_todays_month_and_week():
    today = pd.to_datetime("today")
    month = today.month
    week = today.week
    return month, week
    
def get_current_month_top_performers():
    data = get_all_csv_data()
    current_month_and_week = "-".join([str(i) for i in get_todays_month_and_week()])
    current_month_and_week_data = data[data["month and week"] == current_month_and_week]
    top_performers = current_month_and_week_data.sort_values(by="avg gain/loss ratio", ascending=False).head(3)[["name", "avg gain/loss ratio"]].values.tolist(
    )[0:3]
    return top_performers

if __name__ == "__main__":
    # create_comprehensive_seasonal_csv()
    a = get_current_month_top_performers()
    print(a)