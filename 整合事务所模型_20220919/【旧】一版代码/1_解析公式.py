import os
import re

import numpy as np
import pandas as pd
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
    root_path = "/Users/zaczhang47/Desktop/pythonDQ/整合事务所模型_20220414/"


signlist_operation = ['+', '-', '*', '/', '(', ')']
signlist_relation = ['!=', '<>', '=', '==', '>', '>=', '<', '<=', 'contains']
signlist_logic = ['&', '|']


## funcions
def read_df(file_name):
    # df = pd.read_excel(root_path + url_pacpath + file_name).dropna(axis=0, how='all')
    df = pd.read_excel(root_path + '/会所专题底层数据_220919/' + file_name).dropna(axis=0, how='all')
    df = df[[x for x in df.columns if 'Unnamed' not in x]].drop(index=0, axis=0)
    return df


# 根据两列dataframe生成字典
def getRuleDict(df, before, after):
    ruleDict = {}
    for i in range(len(df)):
        ruleDict[df[before][i]] = df[after][i]

    return ruleDict


def getConsecutiveDataframe(df, variable_name):
    tmp = df.copy()
    base_variable_name = variable_name.split('^')[0]
    num_year = int(variable_name.split('^')[1])
    tmp.loc[tmp['consecutive'] >= num_year, variable_name] = tmp[base_variable_name].shift(num_year)
    if tmp[variable_name].dtype == 'object':
        tmp.loc[tmp['consecutive'] < num_year, variable_name] = np.nan
    else:
        tmp.loc[tmp['consecutive'] < num_year, variable_name] = np.nan
    return tmp


def getSpecialTreatmentDataframe(df, year_factor_name):
    tmp = df.copy()
    base_variable_name = re.split("[*]", year_factor_name)[0]

    if '*year' in year_factor_name:
        try:
            tmp[base_variable_name] = tmp.apply(lambda x: pd.to_datetime(x[base_variable_name]), axis=1)
            tmp[year_factor_name] = tmp.apply(lambda x: x[base_variable_name].year, axis=1)
        except:
            tmp[year_factor_name] = tmp.apply(lambda x: re.findall('(/d+)[^/d]', str(x[base_variable_name])), axis=1)

    if '*abs' in year_factor_name:
        tmp[year_factor_name] = abs(tmp[base_variable_name])

    return tmp


# 解析公式中的各部分
def decodeComponent(my_df_name, this_item, this_code, this_position):
    logic_checker = 0
    # 如果为符号
    if this_item in signlist_logic:
        this_code += '))' + this_item + '('
        this_position = 0
        logic_checker = 1
    elif this_item in signlist_relation:
        if this_item == '=':
            this_item = '=='
        if this_item == '<>':
            this_item = '!='
        if this_item == 'contains':
            this_item = '.str.contains'
        this_code += ')' + this_item
        this_position = 0
    elif this_item in signlist_operation:
        this_code += this_item
    # 如果为内容
    else:
        if this_position == 0:
            this_code += '('
            this_position = 1
        if this_item.strip('.')[0:1].isdigit() or this_item[0:1] == "'" or this_item[0:1] == "\"":
            this_code += this_item
        elif ':' in this_item:
            item_type = this_item.split(':')[0]
            item_content = this_item.split(':')[1]
            # if item_type == 'constant':
            #     this_code += item_content
            if item_type == 'time':
                if '`' in item_content:
                    item_content_y = int(item_content.split('`')[0])
                    item_content_m = int(item_content.split('`')[1])
                    item_content_d = int(item_content.split('`')[2])
                    this_code += "pd.DateOffset(years={}, months={}, days={})".format(item_content_y, item_content_m, item_content_d)
                else:
                    this_code += "pd.to_datetime({}['{}'].astype(str) + '-01-01')".format(my_df_name, item_content)
            elif item_type == 'nan':
                if item_content == 'true':
                    this_code += '.isnull()'
                elif item_content == 'false':
                    this_code += '.notnull()'
        else:
            this_code += "{}['{}']".format(my_df_name, this_item)

    return this_code, this_position, logic_checker


def getConditionString(my_df_name, first_half, final_index_name):

    restriction_items_list = re.split(',', first_half)
    position_index = 0
    prev_variable_list = []
    special_treatment_list = []
    code_language = ''
    code_language_list = []
    logic_checker = 0
    print(restriction_items_list)
    for item in restriction_items_list:
        if '^' in item:
            prev_variable_list.append(item)
        if '*' in item:
            special_treatment_list.append(item)

        code_language, position_index, logic_checker_plus = decodeComponent(my_df_name, item, code_language, position_index)
        logic_checker += logic_checker_plus

    if logic_checker > 0:
        code_language = '{} = {}[('.format(my_df_name, my_df_name) + code_language + ')'
    else:
        code_language = '{} = {}['.format(my_df_name, my_df_name) + code_language
    if position_index != 0:
        code_language += ')'

    code_language += ']'
    code_language_list.append(code_language)

    code_language = "{}['{}'] = 1".format(my_df_name, final_index_name)
    code_language_list.append(code_language)

    # eval(table_rule.loc[table_rule['数据表名称'] == form_name, 'dataframe'])['count'] = 1

    return [code_language_list, prev_variable_list, special_treatment_list]


# 计算：AF_id_year-AF_id_startdate
# df = df['AF_id_year']-df['AF_id_startdate']
# 转换字段文本——>数据表中的列
def covert_field(my_df_name, my_column):
    if '^' in my_column:
        return my_df_name + "['" + my_column.split('^')[0] + "'].shift(" + my_column.split('^')[1] + ")"
    else:
        return my_df_name + "['" + my_column + "']"


def getCalculateString(my_df_name, my_equation, final_index_name):
    prev_variable_list = []
    special_treatment_list = []
    equation_item_list = re.split('([-+*/()])', my_equation)
    equation_item_list = [x for x in equation_item_list if x != '']
    code_language = "{}['".format(my_df_name) + final_index_name + "']="
    for this_item in equation_item_list:
        if '^' in this_item:
            prev_variable_list.append(this_item)
        if '*' in this_item:
            special_treatment_list.append(this_item)
        if this_item in signlist_operation:
            code_language += this_item
        else:
            code_language += "{}['{}']".format(my_df_name, this_item)

    return [[code_language], prev_variable_list, special_treatment_list]


# 映射关系
# 规则输入：FS_AOAO_factor07;{'标准无保留意见':1,'带强调事项段的无保留意见':2,'保留意见':3,'无法表示意见':4,'否定意见':5};AF_CYD_OPINIONLEVEL
# code: df['AF_CYD_OPINIONLEVEL']=df['FS_AOAO_factor07'].apply(lambda x: severity[x])
def getCorrespondenceString(my_df_name, indicator_code, rule_string, final_index_name):
    dict_code_language = 'dict_tmp =' + rule_string
    code_language = "{}['".format(my_df_name) + final_index_name + "']" + "={}['".format(my_df_name) + indicator_code + "'].apply(lambda x: dict_tmp[x] if x is not np.nan else np.nan)"

    return [[dict_code_language, code_language], [], []]


# 存在类
def getExistString(my_df_name, my_factor):
    code_language_list = []
    code_language = "{}['{}'] = 1".format(my_df_name, my_factor)
    code_language_list.append(code_language)
    return [code_language_list, [], []]


# 统计类
def getStatisticsString(my_df_name, first_half, second_half, my_factor):
    prev_variable_list = []
    special_treatment_list = []
    code_language_list = []
    code_language = ''
    logic_checker = 0

    # 创建条件判断语句
    if first_half != '':
        restriction_list = re.split(',', first_half)
        position_index = 0
        for item in restriction_list:
            if '^' in item:
                prev_variable_list.append(item)
            if '*' in item:
                special_treatment_list.append(item)

            code_language, position_index, logic_checker_plus = decodeComponent(my_df_name, item, code_language, position_index)
            logic_checker += logic_checker_plus

        if logic_checker > 0:
            code_language = '{} = {}[('.format(my_df_name, my_df_name) + code_language + ')'
        else:
            code_language = '{} = {}['.format(my_df_name, my_df_name) + code_language
        if position_index != 0:
            code_language += ')'

        code_language += ']'
        code_language_list.append(code_language)

    # 创建groupby语句
    groupby_items = re.split(':', second_half)
    groupby_index_list = re.split(',', groupby_items[0])
    operation_items_list = re.split('[()]', groupby_items[1])
    operation_type = operation_items_list[0]
    operation_target = operation_items_list[1]

    if operation_type == 'rank' or operation_type == 'min':
        code_language = "{}['{}'] = {}.groupby([".format(my_df_name, my_factor, my_df_name)
    else:
        code_language = '{} = {}.groupby(['.format(my_df_name, my_df_name)

    for item in groupby_index_list:
        if '^' in item:
            prev_variable_list.append(item)
        if '*' in item:
            special_treatment_list.append(item)
        code_language += "'" + item + "'"
        if groupby_index_list.index(item) != len(groupby_index_list) - 1:
            code_language += ','

    code_language += "],as_index=False)"

    # sumif: a = df.groupby(['AF_CYD_factor02','AF_CYD_factor04'])['AF_CMP_IF_'].sum()
    if operation_type == 'sum':
        code_language += "['" + operation_target + "'].sum()"

    # rankif: a = df.groupby(['FS_AOAO_factor04'])['FS_AOAO_factor06'].rank()
    elif operation_type == 'rank':
        code_language += "['" + operation_target + "'].rank(na_option='keep', ascending=False, method='min')"

    # min: dfd['new_B'] = dfd.groupby('A')['B'].transform('min')
    elif operation_type == 'min':
        code_language += "['" + operation_target + "'].transform('min')"

    # coutif: a = df.groupby([df1['df11'].shift(1)])['FS_AOAO_factor04'].count()
    elif operation_type == 'count':
        extra_line = "{}['groupby_item_copy'] = {}['".format(my_df_name, my_df_name) + groupby_index_list[0] + "']"
        code_language_list.append(extra_line)
        code_language += "['groupby_item_copy'].count()"

    code_language_list.append(code_language)

    # df.rename(columns={'a':'A',"b":"B"})
    if operation_type == 'count':
        code_language = "{}['".format(my_df_name) + my_factor + "'] = {}['groupby_item_copy']".format(my_df_name)
    elif operation_type == 'rank':
        code_language = ''
    elif operation_type == 'min':
        code_language = ''
    else:
        code_language = "{}['".format(my_df_name) + my_factor + "'] = {}['".format(my_df_name) + operation_target + "']"

    code_language_list.append(code_language)

    return [code_language_list, prev_variable_list, special_treatment_list]


# df = df[(df['year'] == base_year) & (df['MG_MAR_factor07'] != '失败')]
def parseRule(my_df_name, my_rule, my_rule_type, my_factor, form_name):
    eval_string_list = []
    if my_rule is not np.nan:
        first_half = [x.strip(',') for x in re.split(';', my_rule)][0]
        if ';' in my_rule:
            second_half = [x.strip(';') for x in re.split(';', my_rule)][1]
    else:
        first_half = ''
        second_half = ''

    if my_rule_type == '计算':
        eval_string_list = getCalculateString(my_df_name, first_half, my_factor)
    elif my_rule_type == '判断':
        eval_string_list = getConditionString(my_df_name, first_half, my_factor)
    elif my_rule_type == '映射':
        eval_string_list = getCorrespondenceString(my_df_name, first_half, second_half, my_factor)
    elif my_rule_type == '存在':
        eval_string_list = getExistString(my_df_name, my_factor)
    elif my_rule_type == '统计':
        eval_string_list = getStatisticsString(my_df_name, first_half, second_half, my_factor)

    return eval_string_list


def getEditedDataframe(form_df, parseRule_return_list):
    previous_variable_list = parseRule_return_list[1]
    special_treatment_list = parseRule_return_list[2]
    if len(previous_variable_list) > 0:
        for i in previous_variable_list:
            form_df = getConsecutiveDataframe(form_df, i)
    if len(special_treatment_list) > 0:
        for i in special_treatment_list:
            form_df = getSpecialTreatmentDataframe(form_df, i)

    return form_df


def getCombinedDataframe(input_df, tmp_df, my_form_name, my_entity, my_factor):
    tmp_main = tmp_df.copy()
    tmp_factor = input_df.copy()

    # 根据使用表，选定合并主键
    if my_entity == '事务所':
        tmp_df_keys = ['AF_id_name', 'AF_id_year']
        if my_form_name == '-':
            input_df_keys = ['AF_id_name', 'AF_id_year']
        else:
            input_df_keys = table_rule.loc[table_rule['dataframe'] == my_form_name, ['事务所主键', '日期主键']].values.tolist()[0]
            if ',' in input_df_keys[0]:
                if my_factor in ['AF_CYD_IF_TWOAUDIT']:
                    input_df_keys[0] = input_df_keys[0].split(',')[1]
                else:
                    input_df_keys[0] = input_df_keys[0].split(',')[0]
    elif my_entity == '上市公司':
        if my_form_name == '-':
            input_df_keys = ['AF_CYD_factor01', 'AF_CYD_factor04']
        else:
            input_df_keys = table_rule.loc[table_rule['dataframe'] == my_form_name, ['上市公司主键', '日期主键']].values.tolist()[0]
        if my_form_name == 'df_DH':
            tmp_df_keys = ['AF_CYD_factor03']
            input_df_keys = ['AF_DH_factor01']
        else:
            tmp_df_keys = ['AF_CYD_factor01', 'AF_CYD_factor04']

    # 根据指标修改特殊情况
    if my_factor in ['AF_factor11', 'AF_factor12', 'AF_factor13']:
        input_df_keys = ['AF_CYD_LASTAUDIT', 'AF_CYD_factor04']

    #print('tmp:', tmp_df_keys)
    #print('input:', input_df_keys)

    # 统一年份格式为int
    if my_form_name != 'df_DH':
        tmp_main[tmp_df_keys[1]] = tmp_main[tmp_df_keys[1]].astype(int)
        tmp_factor[input_df_keys[1]] = tmp_factor[input_df_keys[1]].astype(int)

    # 仅保留一个拥有consecutive
    if 'consecutive' in tmp_factor.columns:
        tmp_factor = tmp_factor.drop(columns=['consecutive'])

    # 两表合并
    if my_factor == 'all':
        tmp_df = pd.merge(tmp_factor, tmp_main, left_on=input_df_keys, right_on=tmp_df_keys, how='right')
    else:
        tmp_df = pd.merge(tmp_factor[input_df_keys + [my_factor]], tmp_main, left_on=input_df_keys, right_on=tmp_df_keys, how='right')

    if input_df_keys != tmp_df_keys:
        tmp_df = tmp_df.drop(columns=input_df_keys)

    return tmp_df



# 获得以上市公司和审计单位为主体的两个中间表雏形
df_init_firm = pd.read_csv(root_path + '/dfs2/df_CYD.csv')
df_init_audit = pd.read_csv(root_path + '/dfs2/df_id.csv')

print('已获得以上市公司和审计单位为主体的两个中间表雏形！')

# 数据表信息
list_audit = pd.read_excel(root_path+'/rules/0_主体覆盖范围.xlsx', sheet_name="主体清单-事务所", header=1)
list_firm = pd.read_excel(root_path+'/rules/0_主体覆盖范围.xlsx', sheet_name="主体清单-上市公司", header=1)

print('已获得两个中间表的主键列表')

# 读取规则
table_rule = pd.read_excel(root_path + '/rules/0_数据表映射及主键.xlsx', header=1)
rule_df = pd.read_excel(root_path + '/rules/1_指标计算规则.xlsx').dropna(axis=0, how='all')

print('已读取【数据表映射及主键】和【指标计算规则】！')

# 生成中间字典
dict_table_name_1 = dict(zip(table_rule['xlsx名称'], table_rule['dataframe']))
dict_table_name_2 = dict(zip(table_rule['数据表名称'], table_rule['dataframe']))

# 预生成源表
for df in table_rule['dataframe']:
    exec('{} = pd.DataFrame()'.format(df))

# 读取源表
# file_name_array = [f for f in os.listdir(root_path + url_pacpath) if f.endswith('.xlsx')]
file_name_array = [f for f in os.listdir(root_path + '/会所专题底层数据_220919/') if f.endswith('.xlsx')]
for file_name in file_name_array:
    file_name = file_name.split('.xlsx')[0]
    df_name = dict_table_name_1[file_name]
    exec("{} = pd.read_csv(root_path + '/dfs2/' + '{}.csv')".format(df_name, df_name))
print('已获取所有底层数据csv文件！')

# 转换规则
list_rules = list(zip(rule_df['代码'], rule_df['主体对象'], rule_df['优先级'], rule_df['计算类型'], rule_df['输入表'], rule_df['指标加工规则']))

# 执行规则
df_tmp_audit = df_init_audit.copy()
df_tmp_firm = df_init_firm.copy()

for this_factor, this_entity, this_priority, this_type, this_form_name, this_rule in list_rules:
    print("=====" + this_factor + "=====")

    if this_form_name != "-":
        this_form_name = dict_table_name_2[this_form_name]

    # 计算指标并存入中间表
    if this_entity == '事务所':
        df = df_tmp_audit.copy()
    elif this_entity == '上市公司':
        df = df_tmp_firm.copy()

    #
    tmp1 = df.copy()

    # 优先级为1，则直接由源表计算至中间表。此时输入表为源表
    if this_priority == 1:
        lvl1_df = eval(this_form_name + '.copy()')
        # 筛选上市公司
        # 筛选上市公司
        if this_entity == '事务所' and this_form_name in ['df_CYD', 'df_tmp_firm'] and this_type == '统计':
            lvl1_df = lvl1_df.merge(list_firm[['证券代码', '年份']], left_on=['AF_CYD_factor01', 'AF_CYD_factor04'], right_on=['证券代码', '年份'], how='inner')
        # 合并
        tmp3 = getEditedDataframe(lvl1_df, parseRule("tmp3", this_rule, this_type, this_factor, this_form_name))

    # 当优先级大于1，则需根据输入表判断是否为中间表merge源表
    else:
        # 如果不需要输入表
        if this_form_name == '-':
            tmp3 = tmp1.copy()
        # 如果输入表有一个，中间表merge源表再计算
        else:
            lvl1_df = eval(this_form_name + '.copy()')
            # 筛选上市公司
            if this_entity == '事务所' and this_form_name in ['df_CYD', 'df_tmp_firm'] and this_type == '统计':
                lvl1_df = lvl1_df.merge(list_firm[['证券代码', '年份']], left_on=['AF_CYD_factor01', 'AF_CYD_factor04'], right_on=['证券代码', '年份'], how='inner')

            tmp3 = getCombinedDataframe(lvl1_df, tmp1, this_form_name, this_entity, 'all')

        tmp3 = getEditedDataframe(tmp3, parseRule("tmp3", this_rule, this_type, this_factor, this_form_name))

    ## 运行解析好的规则
    # 因为string语句中的主体Dataframe名为df
    tmp2 = tmp3.copy()
    print(this_type)
    for i in parseRule("tmp2", this_rule, this_type, this_factor, this_form_name)[0]:
        print(i)
        exec(i)
        # 这里要加上将结果列合并到中间表的函数

    if this_form_name == 'df_tmp_firm':
        tmp2 = tmp2.rename(columns={
            'AF_id_name': 'FS_AOAO_factor04',
            'AF_id_year': 'AF_CYD_factor04'
        })
    tmp = getCombinedDataframe(tmp2, tmp1, this_form_name, this_entity, this_factor)

    # 特殊情况处理
    # AF_CYD_IF_DEFER/本年是否延期披露年报
    if this_factor == 'AF_CYD_IF_DEFER':
        tmp.loc[(tmp['AF_CYD_factor04'] == 2019) & (pd.to_datetime(tmp['AF_CYD_factor05']) <= pd.to_datetime('2020-06-30')), this_factor] = 0
    #
    if this_factor == 'AF_CYD_RANK_FEE':
        tmp['AF_CYD_RANK_FEE'] = tmp.groupby(['FS_AOAO_factor04', 'AF_CYD_factor04'], as_index=False)['AF_CYD_RANK_FEE'].rank(na_option='keep', ascending=True, method='min')

    #
    if this_type in ['判断', '统计']:
        tmp[this_factor] = tmp[this_factor].fillna(0)

    # 去重处理
    tmp = tmp.drop_duplicates().reset_index(drop=True)

    # 跨期指标处理
    if this_rule is not np.nan and '^' in this_rule:
        tmp.loc[tmp['consecutive'] < 1, this_factor] = np.nan

    if this_entity == '事务所':
        df_tmp_audit = tmp.copy()
    elif this_entity == '上市公司':
        df_tmp_firm = tmp.copy()

    print()
    print()


# 保留需要的字段并排序
df_tmp_audit = df_tmp_audit[[x for x in df_tmp_audit.columns if 'AF_id' in x] + [x for x in df_tmp_audit.columns if 'AF_id' not in x and 'AF_factor' not in x] + sorted([x for x in df_tmp_audit.columns if 'AF_factor' in x])]
# 保留需要的主体x年份
df_tmp_audit = df_tmp_audit.merge(list_audit[['会计师事务所', '年份']], left_on=['AF_id_name', 'AF_id_year'], right_on=['会计师事务所', '年份'], how='inner').drop(columns=['会计师事务所', '年份'])
df_tmp_firm = df_tmp_firm.merge(list_firm[['证券代码', '年份']], left_on=['AF_CYD_factor01', 'AF_CYD_factor04'], right_on=['证券代码', '年份'], how='inner').drop(columns=['证券代码', '年份'])



# 存储
df_tmp_audit.to_excel(root_path + "tmp2/事务所表.xlsx", index=False)
df_tmp_firm.to_excel(root_path + "tmp2/上市公司表.xlsx", index=False)
