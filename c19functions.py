
import pandas as pd

def dailySnapshot(df,date_col,value_col):
    if value_col == 'tested':
        col = ['daily_test_rk','abbr','daily_tests','daily_tests_7day_mean','tested']
        order_col = ['daily_test_rk']
    elif value_col == 'positive':
        col = ['daily_positive_rk','abbr','daily_positive','daily_positive_7day_mean','positive']
        order_col = 'daily_positive_rk'
    elif value_col == 'deaths':
        col = ['daily_deaths_rk','abbr','daily_deaths','daily_deaths_7day_mean','deaths']
        order_col = 'daily_deaths_rk'
    elif value_col == 'tested_pm':
        col = ['daily_test_pm_rk','abbr','daily_tests_pm','daily_tests_pm_7day_mean','tested_pm']
        order_col = ['daily_test_pm_rk']
    elif value_col == 'positive_pm':
        col = ['daily_positive_pm_rk','abbr','daily_positive_pm','daily_positive_pm_7day_mean','positive_pm']
        order_col = 'daily_positive_pm_rk'
    elif value_col == 'deaths_pm':
        col = ['daily_deaths_pm_rk','abbr','daily_deaths_pm','daily_deaths_pm_7day_mean','deaths_pm']
        order_col = 'daily_deaths_pm_rk'
    else: 
        col = []
        order_col=''

    df = df[df['date']==date_col][col]
    df = df.sort_values(order_col)

    return df