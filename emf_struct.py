import psycopg2
import json
import emf_helperAggr as emf_help
import sys

"""
select prod, month, year, sum(x.quant), sum(y.quant)
    from sales
        group by prod, month, year ; x,y 
            such that x.prod = prod and s.state = 'NY and x.month = month and x.year = year,
                        y.prod = prod and y.year = year
                    having sum(y.quant) > 10 * sum(x.quant)

select cust,sum(x.sale),sum(y.sale),sum(z.sale)
    from Sales
        group by cust; x,y,z
        such that x.cust = cust and x.state=“NY”, 
                    y.cust = cust and y.state = “CT”,
                        z.cust = cust and z.state = “NJ” 
        having sum(x.sale) > sum(y.sale) and sum(x.sale) > sum(z.sale)


with t1 as (select cust, prod, min(quant) as x_min from sales where state = 'NY' group by cust, prod),
t2 as (select cust, prod, min(quant) as y_min from sales where state = 'NJ' group by cust, prod),
t3 as (select cust, prod, min(quant) as z_min from sales where state = 'CT' group by cust, prod)
select t1.cust , t1.prod, x_min
from t1, t2, t3 
where t1.cust = t2.cust and t2.cust = t3.cust and t1.prod = t2.prod and t2.prod = t3.prod and x_min > y_min and x_min > z_min
 
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

with open('queries/mfQuery.json') as f:
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

extra_st = []
if 'mf' in data:
    for gv in range(1, data['n'] +  1, 1):
        for attr in data['v']:
            r = str(gv) + "_" + attr + " = " + attr
            extra_st.append(r)
    data['st'].extend(extra_st)

print(data['st'])


""" 
Generating all the possible aggregate functions other than
f-vector
"""
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
This dictionary will have all the possible aggregate functions
"""
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

"""
We will be calculating all the aggregate functions identified
by the main aggr keywords for each grouping var --> groupVar (1,2,3 etc)
aggFunctionList --> all the aggregate functions which need to be calculated
we can get these from gV_Aggr.items() which has (key = 1 and value = "1_sum_quant") etc
"""
for groupVar, aggFunctionList in gV_Aggr.items():

    for aggFunc in aggFunctionList:
        """
        for each aggregate function which is present in the aggFuncList, 
        we are getting the parameters required for the Helper Functions
        Boolean values for their respective aggFunc are updated in the Helper Functions
        """

        #performSum function identifies the aggFunc only with sum in 
        # it and also calculates its corresponding count

        if "sum" in aggFunc:
            sum_attr = aggFunc.split('_')[-1]
            emf_help.performSum(rows, groupVar, gV_suchThat, sum_attr,
                            groupByTup, dataBaseStruct, aggFunc, mainResult, aggFunctionDict)

        """
        Checks if avg and sum are present in the aggFunc
        If yes, it directly performs the average
        Else, It calculates, sum and then performs avg
        """
        if "avg" in aggFunc:
            avg_attr = aggFunc.split('_')[-1]
            sum_attr = str(groupVar)+"_sum_"+avg_attr
            count_attr = str(groupVar)+"_count_"+avg_attr
            
            if(aggFunctionDict[sum_attr]):
                emf_help.performAvg(mainResult, aggFunc, sum_attr, count_attr, aggFunctionDict)
            else:
                emf_help.performSum(rows, groupVar, gV_suchThat, avg_attr,
                            groupByTup, dataBaseStruct, sum_attr, mainResult, aggFunctionDict)
                emf_help.performCount(rows, groupVar, gV_suchThat, avg_attr,
                            groupByTup, dataBaseStruct, count_attr, mainResult, aggFunctionDict)           
                emf_help.performAvg(mainResult, aggFunc, sum_attr, count_attr, aggFunctionDict)

        if "min" in aggFunc:
            min_attr = aggFunc.split('_')[-1]
            emf_help.performMin(rows, groupVar, min_attr, aggFunc, gV_suchThat, groupByTup, dataBaseStruct, 
                            mainResult, aggFunctionDict)
        
        if "count" in aggFunc:
            count_attr = aggFunc.split('_')[-1]
            emf_help.performCount(rows, groupVar, gV_suchThat, count_attr, groupByTup, 
                                    dataBaseStruct, aggFunc, mainResult, aggFunctionDict)
        
        if "max" in aggFunc:
            max_attr = aggFunc.split('_')[-1]
            emf_help.performMax(rows, groupVar, max_attr, aggFunc, gV_suchThat, groupByTup, dataBaseStruct, 
                            mainResult, aggFunctionDict)

            
"""
Having String is directly updated with the condition(string)
from the query's json file
"""
havingStr = data['g']

for agg in data['f']:
    if agg in havingStr:
        havingStr = havingStr.replace(agg, "value['" + agg + "']")

file1 = open("emf_out.py", "w")

outstr = f"""
from prettytable import PrettyTable
import json
import sys

with open('queries/mfQuery.json') as f:
    data = json.load(f)

table = PrettyTable()
table.field_names = data['select']
count = 0

for key, value in {mainResult}.items():
    if {havingStr}:
        table_row = []

        for ele in key:
            table_row.append(ele)
        
        for projAttr in data['select']:
            for k, val in value.items():
                if(projAttr == k):
                    table_row.append(val)

        count += 1
        if "max" or "min" in value.keys():
            if (sys.maxsize in value.values() or 0 in value.values()):
                pass
            else:
                table.add_row(table_row)
        else:
            table.add_row(table_row)
print(table)
print(count)

"""
L = [outstr]

file1.writelines(L)
file1.close()






