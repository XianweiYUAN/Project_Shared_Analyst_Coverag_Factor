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
# 倘若使用稠密矩阵，会浪费大量空间
# cms=np.zeros((60,34580,29615))

# 因此我先将关系表提取为字典邻接表的形式

# 创建用于配对的字典等
connection = []
colist = set(data['EstPermID'])
for i in range(60):
    connection.append({})
# EstPermID:row_index
cindex_to_id = dict(enumerate(set(data['EstPermID']), start=0))
company_id = dict([val, key] for key, val in cindex_to_id.items())

# AnalystID:col_index
aindex_to_id = dict(enumerate(set(data['AnalystID']), start=0))
analyst_id = dict([val, key] for key, val in aindex_to_id.items())


# 下面的数据处理都使用多进程

# 多进程函数1：将每个月公司与分析师的对应关系提取到邻接表
def task1(time):
    year = time[0]
    month = time[1]
    connection = {}
    ym = datetime.datetime(year, month, 1, 0, 0)
    # print(ym)
    conditioned_data = data[(data['StartDate'] <= ym) | (data['EndDate'] >= ym)]
    conditioned_data = conditioned_data[['EstPermID', 'AnalystID']].set_index('EstPermID')
    IDset = set(conditioned_data.index.get_level_values('EstPermID').tolist())
    for id in IDset:
        df = conditioned_data.loc[id]
        analysts = (df['AnalystID'])
        connection[id] = []
        # print(analysts)
        for analyst in np.nditer(analysts):
            # print(analyst)
            analyst = int(analyst)
            # analyst_id[int(analyst)]
            connection[id].append(analyst)
    return connection
    # cms[(year - 2014) * 12 + month - 9, company_id[id], analyst_id[int(analyst)]] = 1
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


# 多进程执行任务一
p = mp.Pool(8)

timeline = []
for i in range(2014, 2020):
    if i == 2014:
        for j in range(9, 13):
            timeline.append([i, j])
    if i == 2019:
        for j in range(1, 10):
            timeline.append([i, j])
    else:
        for j in range(1, 13):
            timeline.append([i, j])
years = []
months = []
for i in range(60):
    years.append(timeline[i][0])
    months.append(timeline[i][1])
square_result = p.map(task1, timeline)
for i in range(60):
    connection[i] = square_result[i]
p.close()
p.join()

str_series = []
time_series = pd.date_range('10/31/2014', '10/1/2019', freq='M')
print(time_series)
for i in time_series:
    tmp = str(i)
    str_series.append(tmp[0:7])

# 导出邻接表数据
for i in range(60):
    f = open('data/' + str_series[i] + ".csv", "w")
    fcsv = csv.writer(f)
    fcsv.writerows(connection[i].items())
    f.close()


# 多进程函数2：将上面到关系表转为稀疏矩阵（否则太大），并用转置乘自身得到相关性矩阵（横纵轴都为公司）
def task2(i):
    pre_connection = connection[i]
    # 使用dok，便于按索引访问并修改
    smatrix = sparse.dok_matrix((34580, 29615))
    companies = pre_connection.keys()
    for c in companies:
        c_id = company_id[c]
        for a in pre_connection[c]:
            smatrix[c_id, analyst_id[a]] = 1

    connection_matrix = smatrix.getH().dot(smatrix)
    return connection_matrix


# 多进程执行任务二
connection_matrices_list = []
p = mp.Pool(8)
square_result = p.map(task2, list(range(60)))
for i in range(60):
    connection_matrices_list.append(square_result[i])

example = np.zeros((1000, 1000))
c = connection_matrices_list[33]
for i in range(1000):
    for j in range(1000):
        example[i, j] = c[i, j]


# 导出部分数据作为案例
example_df = pd.DataFrame(example, index=[cindex_to_id[x] for x in range(1000)],
                          columns=[cindex_to_id[x] for x in range(1000)])
example_df.to_csv("connection_matrix1.csv")
p.close()
p.join()
