import psycopg2
import json
import emf_helperAggr as emf_help

"""
select prod, month, year, sum(x.quant), sum(y.quant)
    from sales
        group by prod, month, year ; x,y 
            such that x.prod = prod and x.month = month and x.year = year,
                        y.prod = prod and y.year = year
                    having sum(y.quant) > 10 * sum(x.quant)

"""

try:
    connect_str = "dbname='postgres' user='postgres' host='localhost' "
    conn = psycopg2.connect(connect_str)
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)

cursor = conn.cursor()
cursor.execute("""SELECT * from sales""")
rows = cursor.fetchall()

with open('query.json') as f:
    data = json.load(f)

key = tuple(data['v'])

dataBaseStruct = {
    "cust": 0,
    "prod": 1,
    "day": 2,
    "month": 3,
    "year": 4,
    "state": 5,
    "quant": 6
}


"""
Getting all the aggregate functions to be computed - Boolean values for each of them
"""
aggFunctionDict = {}
for function in data['f']:
    aggFunctionDict[function] = False



"""
Grouping variables with their corresponding aggregate functions
"""
gV_Aggr = {}
for aggr in data['f']:
    for groupVar in range(1, data['n']+1):
        if str(groupVar) in aggr:
            if groupVar in gV_Aggr:
                gV_Aggr[groupVar].append(aggr)
            else:
                gV_Aggr[groupVar] = [aggr]


"""
Grouping variables with their such that conditions
"""
gV_suchThat = {}
for aggr in data['st']:
    for groupVar in range(1, data['n']+1):
        if str(groupVar) in aggr:
            if groupVar in gV_suchThat:
                gV_suchThat[groupVar].append(aggr)
            else:
                gV_suchThat[groupVar] = [aggr]


"""
Group by attributes indexes - a tuple of indexes
"""
groupByTup = ()
for attrib in data['v']:
    if attrib in dataBaseStruct.keys():
        groupByTup = groupByTup + (dataBaseStruct[attrib],)

"""
EMF Structure
"""
mainResult = {}

"""
Generating the Structure according to the grouping 
attributes along with thier aggregate functions
"""
for row in rows:
    groupList = []
    for index in groupByTup:
        groupList.append(row[index])
    key = tuple(groupList)
    
    if key not in mainResult.keys():
        mainResult[key] = {}
        for aggrFunction in data['f']:
            mainResult[key][aggrFunction] = 0


for groupVar, aggFunctionList in gV_Aggr.items():

    for aggFunc in aggFunctionList:
        if "sum" in aggFunc:
            sum_attr = aggFunc.split('_')[-1]
            emf_help.performSum(rows, groupVar, gV_suchThat, sum_attr,
                            groupByTup, dataBaseStruct, aggFunc, mainResult, aggFunctionDict)


# for key, value in mainResult.items():
#     print(key, value)

havingStr = data['g']

for agg in data['f']:
    if agg in havingStr:
        havingStr = havingStr.replace(agg, "value['" + agg + "']")



file1 = open("emf_out.py", "w")

outstr = f"""
from prettytable import PrettyTable
import json

with open('query.json') as f:
    data = json.load(f)

table = PrettyTable()
table.field_names = data['select']

for key, value in {mainResult}.items():
    if {havingStr}:
        table_row = []

        for ele in key:
            table_row.append(ele)
        
        for projAttr in data['select']:
            for k, val in value.items():
                if(projAttr == k):
                    table_row.append(val)

        table.add_row(table_row)
print(table)

"""
L = [outstr]

file1.writelines(L)
file1.close()






