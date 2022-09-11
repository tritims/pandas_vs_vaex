from traceback import print_tb
import pandas as pd
import numpy as np

FOLDER_PATH = './vaex/missing_data'
df = pd.read_csv(f'{FOLDER_PATH}/trial_vaex_md_1.csv')
print(df)


def generateMap(df):
    map = {}
    for i, r in df.iterrows():
        map[i] = r['tag']
    return map

def bundleValues(folderPath, fileName, start, stop, category):
    map = {}
    for i in range(start, stop+1):
        df = pd.read_csv(f"{folderPath}/{fileName}_{i}.csv")
        for j, r in df.iterrows():
            if j in map:
                map[j].append(r[category])
            else:
                map[j] = [r[category]]
    return map

def aggregate(bundledValues, kv, aggregateFunc):
    map = {}
    for i in bundledValues:
        map[kv[i]] = aggregateFunc(bundledValues[i])
    return map

def generateReport(categories, kv, folderPath, fileName, start, stop, report=[], aggregateFuncs=[]):
    for c in categories:
        bv = bundleValues(folderPath=folderPath, fileName=fileName, start=start, stop=stop, category=c)
        report.append(aggregate(bundledValues=bv, kv=kv, aggregateFunc=np.mean)) # change aggregate function for different report
    return pd.DataFrame(report)

kv = generateMap(df)
categories = ['package_0', 'dram_0', 'core_0', 'uncore_0']

report = generateReport(categories, kv, FOLDER_PATH, 'trial_vaex_md', 1, 10)
report.to_csv('./Reports/vaex_md_report.csv')

# print(bundleValues('./pandas', 'trial', 1, 10, 'core_0'))
# print(bundleValues('./pandas', 'trial', 1, 10, 'dram_0'))
# for c in categories:
#     bv = bundleValues('./pandas', 'trial', 1, 10, c)
#     aggregate(bv, kv, np.min)
# print()
