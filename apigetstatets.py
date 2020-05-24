import pandas as pd
import os
import subprocess

cwd = os.getcwd()
filepath = cwd + '/data/populationdata.csv'
apiscriptpath = cwd + "/apiget.sh"

df = pd.read_csv(filepath, delimiter='|')

abbr = df['abbr']

for state in abbr:
    try:
        subprocess.run([apiscriptpath, state])
    except:
        print(state + " failed api pull")

print(df.head(1))