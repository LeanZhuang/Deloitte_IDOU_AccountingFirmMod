import pandas as pd
import numpy as np
import os
import re
from modules import general


## choose user
user = "XL"

## global varibales
root_path = ""

if user == "XL":
    root_path = r"D:/Deloitte/财报智评Python_会计师事务所专题/整合事务所模型/"
else:
    root_path = "/Users/zaczhang47/Desktop/pythonDQ/整合事务所模型/"


def read_df(file_name):
    df = pd.read_excel(root_path+'/会所专题底层数据_220321/' + file_name).dropna(axis=0,how='all')
    df = df[[x for x in df.columns if 'Unnamed' not in x]].drop(index=0, axis=0)
    return df


def save_df(df_name):
    exec("{}.to_csv(root_path + '会所专题底层数据_220321/{}.txt', index=False, sep=',')".format(df_name, df_name))


######
df0 = pd.read_excel(root_path + "other/人工宽表合并（2018-2020）-20200331(1).xlsx", header=1)
df1 = pd.read_excel(root_path + "tmp/上市公司表.xlsx")
main_cols = ['AF_CYD_factor01', 'AF_CYD_factor04']
######


# AF_CYD_FEEGROWTH
tmp = df1[main_cols + ['FS_AOAO_factor06']]
tmp = tmp[tmp['AF_CYD_factor01'] == '000422.SZ']

# AF_CYD_IF_FEECHANGE
tmp0 = df0[main_cols + ['AF_CYD_FEEGROWTH', 'AF_CYD_IF_FEECHANGE']].rename(columns={'AF_CYD_IF_FEECHANGE': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_FEEGROWTH', 'AF_CYD_IF_FEECHANGE']].rename(columns={'AF_CYD_IF_FEECHANGE': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000004.SZ']

# AF_CYD_IF_DEFAULTER
tmp0 = df0[main_cols + ['AF_CYD_IF_DEFAULTER']].rename(columns={'AF_CYD_IF_DEFAULTER': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_DEFAULTER']].rename(columns={'AF_CYD_IF_DEFAULTER': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000732.SZ']

# AF_CYD_IF_DEFER
tmp0 = df0[main_cols + ['AF_CYD_IF_DEFER']].rename(columns={'AF_CYD_IF_DEFER': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_DEFER']].rename(columns={'AF_CYD_IF_DEFER': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000005.SZ']

# AF_CYD_IF_FIRMCHANGE
tmp0 = df0[main_cols + ['AF_CYD_IF_FIRMCHANGE']].rename(columns={'AF_CYD_IF_FIRMCHANGE': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_FIRMCHANGE']].rename(columns={'AF_CYD_IF_FIRMCHANGE': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '603682.SH']

# AF_CYD_IF_LASTFIRMCHANGE
tmp0 = df0[main_cols + ['AF_CYD_IF_LASTFIRMCHANGE']].rename(columns={'AF_CYD_IF_LASTFIRMCHANGE': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_LASTFIRMCHANGE']].rename(columns={'AF_CYD_IF_LASTFIRMCHANGE': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '603682.SH']

# AF_CYD_IF_REORGANIZATION
tmp0 = df0[main_cols + ['AF_CYD_IF_REORGANIZATION']].rename(columns={'AF_CYD_IF_REORGANIZATION': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_REORGANIZATION']].rename(columns={'AF_CYD_IF_REORGANIZATION': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000100.SZ']

# AF_CYD_IF_SAMECPA
tmp0 = df0[main_cols + ['AF_CYD_IF_SAMECPA']].rename(columns={'AF_CYD_IF_SAMECPA': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_SAMECPA']].rename(columns={'AF_CYD_IF_SAMECPA': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '002936.SZ']

# AF_CYD_IF_TWOAUDIT
tmp0 = df0[main_cols + ['AF_CYD_IF_TWOAUDIT']].rename(columns={'AF_CYD_IF_TWOAUDIT': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_TWOAUDIT']].rename(columns={'AF_CYD_IF_TWOAUDIT': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000153.SZ']

# AF_CYD_IF_VIOLATION
tmp0 = df0[main_cols + ['AF_CYD_IF_VIOLATION']].rename(columns={'AF_CYD_IF_VIOLATION': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_VIOLATION']].rename(columns={'AF_CYD_IF_VIOLATION': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000001.SZ']

# AF_CYD_RANK_FEE
tmp0 = df0[main_cols + ['AF_CYD_RANK_FEE']].rename(columns={'AF_CYD_RANK_FEE': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_RANK_FEE']].rename(columns={'AF_CYD_RANK_FEE': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '000001.SZ']

dff = pd.read_csv(root_path + "dfs/df_CYD.csv")
list_firm = pd.read_excel(root_path+'/rules/0_主体覆盖范围.xlsx', sheet_name="主体清单-上市公司", header=1)
dff = dff.merge(list_firm[['证券代码', '年份']], left_on=['AF_CYD_factor01', 'AF_CYD_factor04'], right_on=['证券代码', '年份'], how='inner')
dff = dff[(dff['AF_CYD_factor04'] == 2020) & (dff['FS_AOAO_factor04'] == '普华永道中天会计师事务所')]
dff = dff.sort_values(by=['FS_AOAO_factor06'], ascending=False).reset_index(drop=True)

# AF_CYD_IF_OPINIONDECREASE
tmp0 = df0[main_cols + ['AF_CYD_IF_OPINIONDECREASE']].rename(columns={'AF_CYD_IF_OPINIONDECREASE': 'excel'})
tmp1 = df1[main_cols + ['AF_CYD_IF_OPINIONDECREASE']].rename(columns={'AF_CYD_IF_OPINIONDECREASE': 'python'})

tmp = tmp0.merge(tmp1)
tmp = tmp[tmp['AF_CYD_factor01'] == '688051.SH']