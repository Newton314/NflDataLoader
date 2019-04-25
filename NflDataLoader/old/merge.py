import pandas as pd
from os import listdir

''' merges all available seasons in one big data file '''
PATH = 'database/seasons'
l = listdir(PATH)
l.sort()
df = pd.DataFrame()
for item in l:
    item = PATH + '/' + item
    temp = pd.read_csv(item)
    df = df.append(temp, ignore_index=True)
df.to_csv('database/data.csv')
