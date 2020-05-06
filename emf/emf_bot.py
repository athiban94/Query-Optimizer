
import psycopg2
import json
import emf_helperAggr as emf_help
import sys
from prettytable import PrettyTable

'''
Database Connection
'''
try:
    connect_str = "dbname='postgres' user='postgres' host='localhost' "
    conn = psycopg2.connect(connect_str)
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)

cursor = conn.cursor()
cursor.execute('''SELECT * from sales''')
rows = cursor.fetchall()

with open('queries/mq.json') as f:
    data = json.load(f)

dataBaseStruct = {'cust': 0, 'prod': 1, 'day': 2, 'month': 3, 'year': 4, 'state': 5, 'quant': 6}


extra_st = []
if 'mf' in data:
    for gv in range(1, data['n'] +  1, 1):
        for attr in data['v']:
            r = str(gv) + '_' + attr + ' = ' + attr
            extra_st.append(r)

    data['st'].extend(extra_st)


'''
Generating all the possible aggregate functions other than
f-vector
'''
extraFunc = []
for func in data['f']:
    gV = func.split('_')[0]
    gAtr = func.split('_')[-1]
    if "avg" in func:
        if not gV+"_sum_"+gAtr in data['f']:
            extraFunc.append(gV+"_sum_"+gAtr)
        if not gV+"_count_"+gAtr in data['f']:
            extraFunc.append(gV+"_count_"+gAtr)
    if "sum" in func:
        if not gV+"_count_"+gAtr in data['f']:
            extraFunc.append(gV+"_count_"+gAtr)

data['f'].extend(extraFunc)

'''
Getting all the aggregate functions to be computed - Boolean values for each of them
'''
aggFunctionDict = {}
for function in data['f']:
    aggFunctionDict[function] = False

'''
Grouping variables with their corresponding aggregate functions
'''
gV_Aggr = {}
for aggr in data['f']:
    for groupVar in range(1, data['n']+1):
        if str(groupVar) in aggr:
            if groupVar in gV_Aggr:
                gV_Aggr[groupVar].append(aggr)
            else:
                gV_Aggr[groupVar] = [aggr]

'''
Grouping variables with their such that conditions
'''
gV_suchThat = {}
for aggr in data['st']:
    aggr_one = aggr.split('=')[0]
    for groupVar in range(1, data['n']+1):
        if str(groupVar) in aggr_one:
            if groupVar in gV_suchThat:
                gV_suchThat[groupVar].append(aggr)
            else:
                gV_suchThat[groupVar] = [aggr]

'''
Group by attributes indexes - a tuple of indexes
'''
groupByTup = ()
for attrib in data['v']:
    if attrib in dataBaseStruct.keys():
        groupByTup = groupByTup + (dataBaseStruct[attrib],)

'''
EMF Structure
'''
mainResult = {}

'''
Generating the Structure according to the grouping 
attributes along with thier aggregate functions
This dictionary will have all the possible aggregate functions
'''
for row in rows:
    groupList = []
    for index in groupByTup:
        groupList.append(row[index])
    key = tuple(groupList)
    
    if key not in mainResult.keys():
        mainResult[key] = {}
        for aggrFunction in data['f']:
            if "min" in aggrFunction:
                mainResult[key][aggrFunction] = sys.maxsize
            else:
                mainResult[key][aggrFunction] = 0

'''
We will be calculating all the aggregate functions identified
by the main aggr keywords for each grouping var --> groupVar (1,2,3 etc)
aggFunctionList --> all the aggregate functions which need to be calculated
we can get these from gV_Aggr.items() 
which has (key = 1 and value = "1_sum_quant") etc
'''

for groupVar, aggFunctionList in gV_Aggr.items():

    for aggFunc in aggFunctionList:
        
        '''
        for each aggregate function which is present in the aggFuncList, 
        we are getting the parameters required for the Helper Functions
        Boolean values for their respective aggFunc are updated in the Helper Functions
        '''
        
        if "sum" in aggFunc:
            sum_attr = aggFunc.split('_')[-1]
            emf_help.performSum(rows, groupVar, gV_suchThat, sum_attr,
                            groupByTup, dataBaseStruct, aggFunc, mainResult, aggFunctionDict)


        if "count" in aggFunc:
            count_attr = aggFunc.split('_')[-1]
            emf_help.performCount(rows, groupVar, gV_suchThat, count_attr, groupByTup, 
                                    dataBaseStruct, aggFunc, mainResult, aggFunctionDict)
        



'''
Generating output table
'''
table = PrettyTable()
table.field_names = data['select']

for key, value in mainResult.items():
    
    table_row = []

    for ele in key:
        table_row.append(ele)
    
    for projAttr in data['select']:
        for k, val in value.items():
            if(projAttr == k):
                table_row.append(val)
    
    # if "max" or "min" in value.keys():
    #     if (sys.maxsize in value.values() or 0 in value.values()):
    #         pass
    #     else:
    #         table.add_row(table_row)
    # else:
    table.add_row(table_row)


print(table)






