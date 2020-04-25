import psycopg2
import json
import helperAggr as helper


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

mainResult = {}

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
Group by attributes indexes
"""
groupByTup = ()
for attrib in data['v']:
    if attrib in dataBaseStruct.keys():
        groupByTup = groupByTup + (dataBaseStruct[attrib],)

"""
Creating separate Data Structures for each aggregate function
"""
for groupVar, value in gV_Aggr.items():
    for aggr in value:

        if "sum" in aggr:
            sum_attr = aggr.split('_')[-1]
            helper.performSum(rows, groupVar, gV_suchThat, sum_attr,
                            groupByTup, dataBaseStruct, aggr, mainResult, aggFunctionDict)
        if "avg" in aggr:
            avg_attr = aggr.split('_')[-1]
            if str(groupVar)+"_sum_"+avg_attr in aggFunctionDict.keys():
                if aggFunctionDict[str(groupVar)+"_sum_"+avg_attr] == True:
                    sum_key = str(groupVar)+"_sum_"+avg_attr
                    count_key = str(groupVar)+"_count_"+avg_attr
                    helper.performAvg(mainResult, aggr, sum_key, count_key)
                    pass
                else:
                    # performSum
                    # performAvg
                    pass
            else:
                # performSum
                # performAvg
                pass


havingStr = data['g']

for agg in data['f']:
    if agg in havingStr:
        havingStr = havingStr.replace(agg, "value['" + agg + "']")


cursor.close()
conn.close()

# return finalresult


file1 = open("out.py", "w")

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
