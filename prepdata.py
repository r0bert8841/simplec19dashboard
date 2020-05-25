import pandas as pd
import os

cwd = os.getcwd()
filepath = cwd + '/data/combined_ts.csv'

df = pd.read_csv(filepath, delimiter=',')
df = df.fillna(0)
print(df.head(5))

