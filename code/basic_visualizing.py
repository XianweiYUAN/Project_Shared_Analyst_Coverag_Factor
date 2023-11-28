import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df=pd.read_csv('connection_matrix.csv',index_col=0)

mat=df.values
plt.matshow(mat,cmap=plt.cm.Blues)
plt.xlabel('Company Index')
plt.ylabel('Company Index')
plt.title('Connection matrix of the first 100 companies')
cb=plt.colorbar()
plt.show()