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


# 读取指标触发规则
# df_rules = pd.read_excel(root_path + "/rules/2_指标触发规则.xlsx", engine='openpyxl', dtype=object).dropna(how='all')
df_rules = pd.read_excel(root_path + "/rules/2_指标触发规则的副本.xlsx", engine='openpyxl', dtype=object).dropna(how='all')

# list_rules = list(zip(df_rules['指标代码'], zip(df_rules['value_i_lower'], df_rules['value_i_upper']), zip(df_rules['value_ii_lower'], df_rules['value_ii_lower'])))
list_rules = list(zip(df_rules['指标代码'], zip(df_rules['value_i_lower'], df_rules['value_i_upper']), zip(df_rules['value_ii_lower'], df_rules['value_ii_upper'])))
del df_rules

# 读取指标值
# df = pd.read_excel(root_path + "/tmp2/事务所表.xlsx")
df = pd.read_excel(root_path + "/tmp2/事务所表_副本.xlsx")
df = df[['AF_id_name', 'AF_id_year'] + [x[0] for x in list_rules]]

# 准备触发表
df0 = df.copy()
df[[x[0] for x in list_rules]] = 0

# 计算触发
for this_factor, (value_i_lower, value_i_upper), (value_ii_lower, value_ii_upper) in list_rules:
    # 如有普通触发
    if value_i_lower != '-':
        # 单向触发
        if value_i_lower < value_i_upper:
            df.loc[(df0[this_factor] >= value_i_lower) & (df0[this_factor] < value_i_upper), this_factor] = 1
        # 两侧触发
        elif value_i_lower > value_i_upper:
            df.loc[(df0[this_factor] >= value_i_lower) | (df0[this_factor] < value_i_upper), this_factor] = 1
    # 如有严重触发
    if value_ii_lower != '-':
        # 单向触发
        if value_ii_lower < value_ii_upper:
            df.loc[(df0[this_factor] >= value_ii_lower) & (df0[this_factor] < value_ii_upper), this_factor] = 2
        # 两侧触发
        # elif value_i_lower > value_i_upper:
        elif value_ii_lower > value_ii_upper:
            df.loc[(df0[this_factor] >= value_ii_lower) | (df0[this_factor] < value_ii_upper), this_factor] = 2

# 存储
df.to_excel(root_path + "/tmp2/事务所指标触发.xlsx", index=False)