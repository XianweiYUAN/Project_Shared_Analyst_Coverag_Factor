import multiprocessing as mpo
import pathos.multiprocessing as mp
import time_test
import os
import random
import pandas as pd
import numpy as np
import datetime
import csv

from scipy import sparse

data = pd.read_pickle('preprocessed.pkl')
#connection_matrixs
cms=sparse.coo_matrix((34580,29615))
#cms=np.zeros((60,34580,29615))

# 字典邻接表
connection= []
colist=set(data['EstPermID'])
for i in range(60):
    connection.append({})
# EstPermID:row_index
names=dict(enumerate(set(data['EstPermID']),start=0))
company_id=dict([val,key] for key,val in names.items())

# AnalystID:col_index
names=dict(enumerate(set(data['AnalystID']),start=0))
analyst_id=dict([val,key] for key,val in names.items())
# 关联矩阵
#print(analyst_id)
# 多进程函数
def test_task(year,month):
    ym=datetime.datetime(year, month,1,0,0)
    #print(ym)
    conditioned_data=data[(data['StartDate'] <=ym) | (data['EndDate'] >=ym)]
    conditioned_data=conditioned_data[['EstPermID','AnalystID']].set_index('EstPermID')
    IDset = set(conditioned_data.index.get_level_values('EstPermID').tolist())
    for id in IDset:
        df=conditioned_data.loc[id]
        analysts=(df['AnalystID'])
        row=company_id[id]
        cols=list(analysts.apply(lambda x:analyst_id[x]))
        rows=[row]*len(cols)
        values=[1]*len(cols)
        A = sparse.coo_matrix((values, (rows, cols)), shape=(34580,29615))



            #cms[(year - 2014) * 12 + month - 9, company_id[id], analyst_id[int(analyst)]] = 1
    # #company_groups=conditioned_data.groupby(['EstPermID']).indices
    # print(company_groups)
    # #company_groups=dict(company_groups)
    # company_keys=list(company_groups.keys())
    # for i in range(len(company_groups)):
    #     ckey=company_keys[i]
    #     row=company_id[ckey]
    #     analysts=company_groups[ckey]
    #     for j in analysts:
    #         print(company_groups.popitem())
    #         cms[(year-2014)*12+month-9,row,analyst_id[j]]=1


test_task(2014,9)
headers=['1','1','1']
print(connection[0].popitem())
f = open("connection.csv", "w")
fcsv=csv.writer(f)
fcsv.writerows(connection[0].items())
f.close()

# 生成邻接表
p = mp.Pool(8)
for i in range(2014,2020):
    if i==2014:
        for j in range(10,13):
            p.apply_async(test_task, args=(i,j,))
    if i ==2019:
        for j in range(1, 10):
            p.apply_async(test_task, args=(i, j,))
    else:
        for j in range(1,13):
            p.apply_async(test_task, args=(i, j,))
p.close()
p.join()

# 计算相关度
for i in range(60):
    pre_connection=connection[i]

