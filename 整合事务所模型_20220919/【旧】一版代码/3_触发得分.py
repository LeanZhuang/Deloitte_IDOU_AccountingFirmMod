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


# 读取分数转换规则
# df_rules = pd.read_excel(root_path + "/rules/3_指标分数转换.xlsx", engine='openpyxl', dtype=object).dropna(how='all')
df_rules = pd.read_excel(root_path + "/rules/3_指标分数转换_副本.xlsx", engine='openpyxl', dtype=object).dropna(how='all')
df_rules = df_rules[['指标代码', '不触发分值', '普通触发分值', '严重触发分值']].rename(columns={
    '不触发分值': 0,
    '普通触发分值': 1,
    '严重触发分值': 2
})

dict_rules = df_rules.set_index('指标代码').to_dict(orient='index')
del df_rules

# 读取指标值
df = pd.read_excel(root_path + "/tmp2/事务所指标触发.xlsx")

# 准备分数表
df0 = df.copy()

# 转换分数
for this_factor in dict_rules.keys():
    df[this_factor] = df[this_factor].apply(lambda x: dict_rules[this_factor][x])

# 存储
df.to_excel(root_path + "/tmp2/事务所指标得分.xlsx", index=False)