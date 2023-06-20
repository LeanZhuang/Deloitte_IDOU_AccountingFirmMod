import pandas as pd
import numpy as np
import os
import re
from modules import general


## choose user
user = "XL"

## global varibales
root_path = ""

# 保存文件夹名称
root_name='会所专题底层数据_220919'
url_pacpath='/'+root_name+'/'

if user == "XL":
    root_path = r"/整合事务所模型_20220919"
else:
    root_path = "/Users/zaczhang47/Desktop/pythonDQ/整合事务所模型/"


def read_df(file_name):
    # df = pd.read_excel((root_path+url_pacpath) + file_name).dropna(axis=0,how='all')
    df = pd.read_excel(root_path+'/会所专题底层数据_220919/' + file_name).dropna(axis=0,how='all')
    df = df[[x for x in df.columns if 'Unnamed' not in x]].drop(index=0, axis=0)
    return df


def save_df(df_name):
    #  此处名称后续可修改
    exec("{}.to_csv(root_path + '会所专题底层数据_220919/{}.txt', index=False, sep=',')".format(df_name, df_name))


# 按照dataframe两列生成字典
def getRuleDict(my_df, before, after):
    ruleDict = {}
    for i in range(len(my_df)):
        ruleDict[my_df[before][i]] = my_df[after][i]

    return ruleDict


# getAllDataframe()

# 数据表信息
table_rule = pd.read_excel(root_path+'/rules/0_数据表映射及主键.xlsx', header=1)

# 会计师事务所名称映射规则
account_rule_dict = getRuleDict(pd.read_excel(root_path + '/rules/0_会计师事务所名称映射.xlsx'), '映射前', '映射后')
dataframe_rule_dict = getRuleDict(table_rule, 'xlsx名称', 'dataframe')

# 读取文件夹内所有表名
# file_name_array = [f for f in os.listdir(root_path+url_pacpath) if f.endswith('.xlsx')]
file_name_array = [f for f in os.listdir(root_path+'/会所专题底层数据_221117/') if f.endswith('.xlsx')]

#print(file_name_array)

for file_name in file_name_array:
    print(file_name)

    # 读表
    df = read_df(file_name)
    file_name = file_name.split('.xlsx')[0]

    # 生成consecutive
    table_keys = table_rule.loc[table_rule['xlsx名称'] == file_name, ['主体主键', '日期主键']].values.tolist()[0]
    if table_keys[1] is not np.nan and file_name != '【自有】失信人底层数据':
        df = general.gen_consecutive(df, table_keys[0], table_keys[1], 'consecutive')

    # 审计事务所名映射
    account_name = table_rule.loc[table_rule['xlsx名称'] == file_name, ['事务所主键']].values.tolist()[0][0]
    if account_name is not np.nan:
        for this_col in account_name.split(','):
            df[this_col] = df[this_col].apply(lambda x: account_rule_dict[x] if x in account_rule_dict.keys() else x)

    # 清理缺失数据
    df = df.replace('-', np.nan)


    ## 日期转年份
    # 重大重组事件日期
    if file_name == "【WIND】重大重组事件底层数据":
        df['MG_MAR_factor03'] = pd.to_datetime(df['MG_MAR_factor03']).dt.year.fillna(0).astype(int)
    # ST日期
    if file_name == "【WIND】实施ST底层数据":
        df['AF_ST_factor03'] = pd.to_datetime(df['AF_ST_factor03']).dt.year.fillna(0).astype(int)
    # 立案调查开始日期转年份
    if file_name == "【WIND】立案调查底层数据":
        df['AF_LR_MTBI_start_time'] = pd.to_datetime(df['AF_LR_MTBI_start_time']).dt.year.fillna(0).astype(int)
    # 违规事件
    if file_name == "【WIND】违规事件底层数据":
        df['AF_VIO_factor02'] = pd.to_datetime(df['AF_VIO_factor02']).dt.year.fillna(0).astype(int)
    # 开庭日期
    if file_name == "【自有】开庭公告底层数据":
        df['AF_tr_start_date'] = pd.to_datetime(df['AF_tr_start_date']).dt.year.fillna(0).astype(int)
    # 裁决日期
    if file_name == "【自有】裁决文书底层数据":
        df['AF_ws_releasedate'] = pd.to_datetime(df['AF_ws_releasedate']).dt.year.fillna(0).astype(int)
    # 定增日期
    if file_name == "【WIND】上市公司分年份数据":
        df['AF_CYD_factor08'] = pd.to_datetime(df['AF_CYD_factor08']).dt.year.astype(float)
    # 失信人日期
    if file_name == "【自有】失信人底层数据":
        df['AF_DH_factor02'] = df['AF_DH_factor02'].fillna(99999)
        df['AF_DH_factor02'] = df['AF_DH_factor02'].apply(lambda x: int(str(x)[0:4]) if x != 99999 else 99999)

    # 存表
    df.to_csv(root_path + "/dfs2/" + dataframe_rule_dict[file_name] + '.csv', index=False)



