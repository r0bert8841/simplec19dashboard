import pandas as pd
import pandasql as ps

import os

cwd = os.getcwd()
filepath = cwd + '/data/combined_ts.csv'
populationfilepath = cwd + '/data/populationdata.csv'
outputfilepath = cwd + '/data/coronavirus_final.csv'


df = pd.read_csv(filepath, delimiter=',')
population_df = pd.read_csv(populationfilepath, delimiter='|')

# Replace days with no results with 0s
df = df.fillna(0)




# Adding population to the Dataset and calculating the per million values
#pm means per million
query1 = """
SELECT q1.*
    , q2.population 
    , 1000000/q2.population as multipliler
    , tested * 1000000/q2.population as tested_pm
    , positive * 1000000/q2.population as positive_pm
    , deaths * 1000000/q2.population as deaths_pm
FROM df as q1
left outer join population_df as q2
on q1.abbr = q2.abbr
"""

#creates the df from query 1 
result = ps.sqldf(query1, locals())



# Now we need to calculate the daily differences
# test_pm - prev_test_pm = test*2 -  prev_test*2 = (test-prev_test)*a = daily_test_pm
query2 = """
SELECT q1.*
    , tested - coalesce(lag(tested,1) over (partition by abbr order by seconds_since_Epoch ),0) as daily_tests
    , positive - coalesce(lag(positive,1) over (partition by abbr order by seconds_since_Epoch ),0) as daily_positive
    , deaths - coalesce(lag(deaths,1) over (partition by abbr order by seconds_since_Epoch ),0) as daily_deaths
    , tested_pm - coalesce(lag(tested_pm,1) over (partition by abbr order by seconds_since_Epoch ),0) as daily_tests_pm
    , positive_pm - coalesce(lag(positive_pm,1) over (partition by abbr order by seconds_since_Epoch ),0) as daily_positive_pm
    , deaths_pm - coalesce(lag(deaths_pm,1) over (partition by abbr order by seconds_since_Epoch ),0) as daily_deaths_pm
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
    , rank() over (partition by seconds_since_Epoch order by daily_tests_7day_mean desc) as daily_test_rk
    , rank() over (partition by seconds_since_Epoch order by daily_positive_7day_mean desc) as daily_positive_rk
    , rank() over (partition by seconds_since_Epoch order by daily_deaths_7day_mean desc) as daily_deaths_rk
    , rank() over (partition by seconds_since_Epoch order by daily_tests_pm_7day_mean desc) as daily_test_pm_rk
    , rank() over (partition by seconds_since_Epoch order by daily_positive_pm_7day_mean desc) as daily_positive_pm_rk
    , rank() over (partition by seconds_since_Epoch order by daily_deaths_pm_7day_mean desc) as daily_deaths_pm_rk
FROM runningavg as q1
"""

#creates the df from query 3 
final = ps.sqldf(query3, locals())

#write the output csv
final.to_csv(outputfilepath)