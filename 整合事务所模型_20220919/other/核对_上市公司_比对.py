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


#
rule_df = pd.read_excel(root_path + 'rules/1_指标计算规则.xlsx').dropna(axis=0, how='all')
rule_df = rule_df[rule_df['主体对象'] == '上市公司']
list_factors = [x for x in rule_df['代码'] if x != 'AF_CYD_IF_FIRMSTAY']
del rule_df

# excel
df0 = pd.read_excel(root_path + "other/人工宽表合并（2018-2020）-20200331(1).xlsx", header=1)
df0['AF_CYD_factor04'] = df0['AF_CYD_factor04'].astype(int)
df0 = df0.sort_values(by=['AF_CYD_factor01', 'AF_CYD_factor04']).reset_index(drop=True)
df0 = df0[['AF_CYD_factor01', 'AF_CYD_factor04'] + list_factors]

# python
df1 = pd.read_excel(root_path + "tmp/上市公司表.xlsx")
df1 = df1[[x for x in df0.columns]]
df1 = df1.merge(df0[['AF_CYD_factor01', 'AF_CYD_factor04']], on=['AF_CYD_factor01', 'AF_CYD_factor04'], how='inner')

# 调整
df1['AF_CYD_FEEGROWTH'] = df1['AF_CYD_FEEGROWTH'].apply(lambda x: abs(x))

#
df0 = df0.set_index(['AF_CYD_factor01', 'AF_CYD_factor04'])
df1 = df1.set_index(['AF_CYD_factor01', 'AF_CYD_factor04'])

df0 = df0.round(4)
df1 = df1.round(4)

df0 = df0.fillna('missing')
df1 = df1.fillna('missing')

#
df = df0 == df1
df = df.reset_index()
df0 = df0.reset_index()
df1 = df1.reset_index()

#
df = df.replace({
    True: int(1),
    False: '!BAD'
})

df = df.append(df[df == 1.0].count()/df.count(), ignore_index=True)
df = df.append(df[df == "!BAD"].count()/df.count(), ignore_index=True)

df["AF_CYD_factor01"].iloc[-2] = "same"
df["AF_CYD_factor01"].iloc[-1] = "diff"
df["AF_CYD_factor04"].iloc[-2] = "same"
df["AF_CYD_factor04"].iloc[-1] = "diff"

df.to_excel(root_path + "other/比对_上市公司_v6_20220402.xlsx", index=False)