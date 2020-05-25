import pandas as pd
import os
import subprocess


cwd = os.getcwd()
filepath = cwd + '/data/populationdata.csv'
outputfilepath = cwd + '/data/combined_ts.csv'

df = pd.read_csv(filepath, delimiter='|')

abbr = df['abbr']

column_names = ["abbr", "seconds_since_Epoch", "tested", "positive","deaths"]
combined = pd.DataFrame(columns=column_names)

for state in abbr:
    try:
        # Here we pull each states ts file and append it to the above file
        statefilepath= cwd + '/data/State_ts_'+state+'.csv'
        temp_df = pd.read_csv(statefilepath, delimiter=',')
        temp_df['abbr'] = state
        combined=combined.append(temp_df)
        print(state)
    except:
        print(state + " failed to append")

combined.to_csv(outputfilepath)