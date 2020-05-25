import pandas as pd
import pandasql as ps
import datetime

import os

cwd = os.getcwd()
filepath = cwd + '/data/combined_ts.csv'
populationfilepath = cwd + '/data/populationdata.csv'
outputfilepath = cwd + '/data/coronavirus_final.csv'


df = pd.read_csv(filepath, delimiter=',')
population_df = pd.read_csv(populationfilepath, delimiter='|')

# Replace days with no results with 0s
df = df.fillna(0)

## Add datetime and date fields
df['datetime'] = pd.to_datetime(df['seconds_since_Epoch'], unit='s')
df['date'] = df['datetime'].dt.date

# Make it so there is only 1 record per day for each state 
query0 = """
Select abbr, date
    ,max(tested) as tested
    ,max(positive) as positive
    ,max(deaths) as deaths
    ,count(*) as daily_records
From df
group by abbr, date
"""

cleaned = ps.sqldf(query0, locals())


# Now we need to create a date table and join it to the population table
# We need to do this to ensure every state has a record for each day. 
now = datetime.date.today()
current_date = now.strftime("%Y-%m-%d")
date_df = pd.DataFrame({"date": pd.date_range('2020-03-08',current_date)})
date_df['date'] = date_df['date'].dt.strftime("%Y-%m-%d")

# cross join these two tables
query_pop = """
select * from population_df, date_df
"""
population_cleaned = ps.sqldf(query_pop, locals())



# Adding population to the Dataset and calculating the per million values
#pm means per million
# if a day doesn't have values for tested, positive, or deaths, we replace them with the previous days values
query1 = """
SELECT q2.abbr
    , q2.date
    , coalesce(q1.daily_records,0)
    , coalesce(q1.tested, lag(q1.tested,1) over (partition by q2.abbr order by q2.date )) as tested 
    , coalesce(q1.positive, lag(q1.positive,1) over (partition by q2.abbr order by q2.date )) as positive 
    , coalesce(q1.deaths, lag(q1.deaths,1) over (partition by q2.abbr order by q2.date )) as deaths 
    , q2.population 
    , 1000000/q2.population as multipliler
    , coalesce(q1.tested, lag(q1.tested,1) over (partition by q2.abbr order by q2.date )) * 1000000/q2.population as tested_pm
    , coalesce(q1.positive, lag(q1.positive,1) over (partition by q2.abbr order by q2.date )) * 1000000/q2.population as positive_pm
    , coalesce(q1.deaths, lag(q1.deaths,1) over (partition by q2.abbr order by q2.date )) * 1000000/q2.population as deaths_pm
FROM population_cleaned as q2
left outer join cleaned as q1
on q1.abbr = q2.abbr and q1.date=q2.date
"""

#creates the df from query 1 
result = ps.sqldf(query1, locals())



# Now we need to calculate the daily differences
# test_pm - prev_test_pm = test*2 -  prev_test*2 = (test-prev_test)*a = daily_test_pm
query2 = """
SELECT q1.*
    , tested - coalesce(lag(tested,1) over (partition by abbr order by date ),0) as daily_tests
    , positive - coalesce(lag(positive,1) over (partition by abbr order by date ),0) as daily_positive
    , deaths - coalesce(lag(deaths,1) over (partition by abbr order by date ),0) as daily_deaths
    , tested_pm - coalesce(lag(tested_pm,1) over (partition by abbr order by date ),0) as daily_tests_pm
    , positive_pm - coalesce(lag(positive_pm,1) over (partition by abbr order by date ),0) as daily_positive_pm
    , deaths_pm - coalesce(lag(deaths_pm,1) over (partition by abbr order by date ),0) as daily_deaths_pm
FROM result as q1
"""

#creates the df from query 2 
dailydiff = ps.sqldf(query2, locals())




# Calculate the 7 day moving averages
dailydiff['daily_tests_7day_mean']=dailydiff.groupby('abbr').rolling(7)['daily_tests'].mean().reset_index(drop=True)
dailydiff['daily_positive_7day_mean']=dailydiff.groupby('abbr').rolling(7)['daily_positive'].mean().reset_index(drop=True)
dailydiff['daily_deaths_7day_mean']=dailydiff.groupby('abbr').rolling(7)['daily_deaths'].mean().reset_index(drop=True)
dailydiff['daily_tests_pm_7day_mean']=dailydiff.groupby('abbr').rolling(7)['daily_tests_pm'].mean().reset_index(drop=True)
dailydiff['daily_positive_pm_7day_mean']=dailydiff.groupby('abbr').rolling(7)['daily_positive_pm'].mean().reset_index(drop=True)
dailydiff['daily_deaths_pm_7day_mean']=dailydiff.groupby('abbr').rolling(7)['daily_deaths_pm'].mean().reset_index(drop=True)
runningavg = dailydiff.fillna(0)



# Calculate the state rankings for each day
query3 = """
SELECT q1.*
    , rank() over (partition by date order by daily_tests_7day_mean desc) as daily_test_rk
    , rank() over (partition by date order by daily_positive_7day_mean desc) as daily_positive_rk
    , rank() over (partition by date order by daily_deaths_7day_mean desc) as daily_deaths_rk
    , rank() over (partition by date order by daily_tests_pm_7day_mean desc) as daily_test_pm_rk
    , rank() over (partition by date order by daily_positive_pm_7day_mean desc) as daily_positive_pm_rk
    , rank() over (partition by date order by daily_deaths_pm_7day_mean desc) as daily_deaths_pm_rk
FROM runningavg as q1
"""

#creates the df from query 3 
final = ps.sqldf(query3, locals())




#write the output csv
final.to_csv(outputfilepath)