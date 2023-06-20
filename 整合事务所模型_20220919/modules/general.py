import pandas as pd
import numpy as np
import re


## 将公式/关键变量里的原始科目挑出来
# 输入: str，代码公式或代码罗列
# 输出: list，去重、独特的科目代码
def select_components(my_equation):
    # 拆分公式，仅保留非运算符号部分
    tmp = my_equation
    # 标注所有运算符号
    for operator in ['+', '-', '*', '/', '(', ')', ',']:
        tmp = str(tmp).replace(operator, ";")
    # 在标记位置拆分
    lst1 = [x.strip() for x in tmp.split(';') if x]
    lst2 = []
    # 处理'^'符号 (用于标记非本期科目)
    for item in lst1:
        if '^' in item:
            item = item.split('^')[0]
        lst2.append(item)
    return list(set(lst2))


## 检测dataframe是否包含公式中所需全部科目
# 输入: list，去重、独特的科目代码； pd.dataframe， 数据表
# 输出: 0：不满足；1：满足
def check_components(my_equation, my_df):
    my_cols = my_df.columns
    my_check = 1
    lst1 = select_components(my_equation)
    # 检测公式中所需科目是否存在数据列中，如否则报错
    for item in lst1:
        # 如果表头中有该代码
        if item in my_cols:
            my_check *= 1
        else:
            my_check *= 0
            print("Error:", item, "is missing")
    return my_check


# 转换字段文本——>数据表中的列
def covert_field(df_name, my_column):
    if '^' in my_column:
        return df_name + "['" + my_column.split('^')[0] + "'].shift(" + my_column.split('^')[1] + ")"
    else:
        return df_name + "['" + my_column + "']"


# 处理规则公式
def parse_equation(df_name, my_equation):
    i = 0
    operators = ["(", ")", "+", "-", "*", "/"]
    new_equation = ""
    while i < len(my_equation):
        if my_equation[i] in operators:
            new_equation += my_equation[i]
            i += 1
        else:
            j = i
            while my_equation[j] not in operators:
                j += 1
                if j >= len(my_equation):
                    break
            new_equation += covert_field(df_name, my_equation[i:j])
            i = j

    return new_equation


# 判定时间序列
def gen_consecutive(my_df, col_name, col_year, col_consecutive):
    tmp = my_df.copy()

    try:
        tmp['date'] = tmp.apply(lambda x: pd.to_datetime(str(x[col_year])), axis=1)
        tmp['year'] = tmp.apply(lambda x: x['date'].year, axis=1)
    except:
        tmp['date'] = tmp.apply(lambda x: re.findall('(\d+)[^\d]?', str(x[col_name])), axis=1)
        tmp['year'] = tmp.apply(lambda x: re.findall('(\d+)[^\d]?', str(x[col_name])), axis=1)

    # 按照公司名称+年份排序
    tmp = tmp.sort_values(by=[col_name, 'year']).reset_index(drop=True)
    tmp['year'] = tmp['year'].apply(lambda x: 0 if x == '' else x).fillna(0)
    tmp['year'] = tmp['year'].astype(int)
    tmp['year_gap'] = tmp.groupby(col_name)['year'].diff(1)

    # 0: 只有当期数据
    tmp.loc[tmp['year_gap'] != 1, col_consecutive] = 0
    # 1: 只有当期及上期数据
    tmp.loc[(tmp['year_gap'] == 1) & (tmp[col_consecutive].shift(1) == 0), col_consecutive] = 1
    # 2: 有至少连续三期数据
    tmp[col_consecutive] = tmp[col_consecutive].fillna(2)

    # 清理：转为整数，删除临时列
    tmp[col_consecutive] = tmp[col_consecutive].astype(int)
    tmp.drop(columns=['date', 'year', 'year_gap'], inplace=True)
    return tmp