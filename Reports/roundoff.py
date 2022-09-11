import pandas as pd
fname = 'dask_st_report'
df = pd.read_csv(f'{fname}.csv')
columns = df.columns

for c in columns[1:]:
    df[c] = df[c].apply(lambda x: round(x/1000, 1))

df.to_csv(f'./compressed/{fname}_c.csv')