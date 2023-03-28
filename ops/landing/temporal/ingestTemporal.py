import sys
import subprocess
import pandas as pd
import numpy as np
import os, json
import csv
import glob
pd.set_option('display.max_columns', None)


#read lookup tables
lookup1 = pd.read_csv('idealista_extended.csv')
lookup2 = pd.read_csv('income_opendatabcn_extended.csv')


#read json files (idealista)

df_j = pd.DataFrame()

jsonpath = '\Users\usuario\Desktop\Master UPC\-[BDM]Big Data Management\Project\P1\data\idealista\*' #check with yours pls
jsonpattern = os.path.join(jsonpath,'*.json')
filelist1 = glob.glob(jsonpattern)

dfs_j = [] #shove the jsons in here bruh

for file in filelist1:
    jsondata = pd.read_json(file, lines=True)
    dfs_j.append(jsondata)

df_j = pd.concat(dfs_j, ignore_index=True)


#read csv files (income)

df_c = pd.DataFrame()

csvpath = '\Users\usuario\Desktop\Master UPC\-[BDM]Big Data Management\Project\P1\data\opendatabcn-income\*' #check with yours pls 2
csvpattern = os.path.join(csvpath, '*.csv')
filelist2 = glob.glob(csvpattern)

dfs_c = [] #shove csvs in here

for file in filelist2:
    csvdata = pd.read_csv(file, lines=True)
    dfs_c.append(csvdata)

df_c = pd.concat(dfs_c, ignore_index=True)


#merge data
# merge lookup1 with df_j
## merged_df_j = pd.merge(df_j, lookup1, on="nofuckingclue")

#merge lookup2 with df_c
## merged_df_c = pd.merge(df_c, lookup2, on="nofuckingclue")





#file = sys.argv[1]
#print(file)
