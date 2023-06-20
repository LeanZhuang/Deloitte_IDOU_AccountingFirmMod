import os
import re

import numpy as np
import pandas as pd
from modules import general

## choose user
user = "XL"

## global varibales
root_path = ""

if user == "XL":
    root_path = r"/整合事务所模型_20220919"
else:
    root_path = "/Users/zaczhang47/Desktop/pythonDQ/整合事务所模型/"


# 读取指标权重
# df_rules = pd.read_excel(root_path + "/rules/4_指标维度映射.xlsx", engine='openpyxl', dtype=object).dropna(how='all')
df_rules = pd.read_excel(root_path + "/rules/4_指标维度映射_副本.xlsx", engine='openpyxl', dtype=object).dropna(how='all')
dict_rules = dict(zip(df_rules['指标代码'], df_rules['单指标权重']))
del df_rules

# 读取分数
df = pd.read_excel(root_path + "/tmp2/事务所指标得分.xlsx")

# 准备分数表
df0 = df.copy()

# 转换分数
for this_factor in dict_rules.keys():
    df[this_factor] = df[this_factor].apply(lambda x: dict_rules[this_factor] * x)

# 加权
df['score'] = df[[x for x in df.columns if 'AF_factor' in x]].sum(axis=1)

#
df = df[['AF_id_name', 'AF_id_year', 'score']]

# 存储
df.to_excel(root_path + "/tmp2/事务所总分.xlsx", index=False)