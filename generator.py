
import psycopg2
import json
import pandas as pd

def performSum(result, rows, groupbyTup, attrIndex, aggr):

    freqStr = 'freq_' + aggr.split('_')[-1]
    for row in rows:
        groupAttriList = []
        for index in groupbyTup:
            groupAttriList.append(row[index])
        
        key = tuple(groupAttriList)
        if key in result.keys():
            result[key][aggr] += row[attrIndex]
            result[key][freqStr] += 1
        else:
            result[key] = {
                aggr    : row[attrIndex],
                freqStr : 1
            }

def performAvg(result, aggr):
    sumStr = "sum_"+aggr.split('_')[-1]
    freqStr = "freq_"+aggr.split('_')[-1]
    for key, value in result.items():
        value[aggr]= value[sumStr]/value[freqStr]

try:
    connect_str = "dbname='postgres' user='postgres' host='localhost' "
    conn = psycopg2.connect(connect_str)

    cursor = conn.cursor()
    cursor.execute("""SELECT * from sales""")
    rows = cursor.fetchall()

    with open('query.json') as f:
        data = json.load(f)
 
    key = tuple(data['v'])

    dataBaseStruct = {
        "cust" : 0,
        "prod" : 1,
        "day" : 2,
        "month" : 3,
        "year" : 4,
        "state" : 5,
        "quant" : 6
    }

    aggFunctionDict = {}

    for function in data['f']:
        aggFunctionDict[function] = False
    
    print(aggFunctionDict)

    tup = []
    for attrib in data['v']:
        if attrib in dataBaseStruct.keys():
            tup.append(dataBaseStruct[attrib])

    mainResult = {}
    for aggr in data['f']:
        if  "sum" in aggr:
            sum_on_attr = aggr.split('_')[-1]
            performSum(mainResult, rows, tuple(tup), dataBaseStruct[sum_on_attr], aggr)
            aggFunctionDict[aggr] = True
        
        if "avg" in aggr:
            avg_on_attr = aggr.split('_')[-1]
            if "sum_"+avg_on_attr in aggFunctionDict.keys():
                performAvg(mainResult, aggr)
            else:
                performSum(mainResult, rows, tuple(tup), dataBaseStruct[sum_on_attr], aggr)
                performAvg(mainResult, aggr)

    for key, value in mainResult.items():
        for conditions in data['g']:
            print(type(conditions))
        


    cursor.close()
    conn.close()


except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)


"""
        result = {
            cust_prod : {
                
                (Emily, milk) : {
                1_sum_quant: 2000
                2_sum_quant: 2000
                avg_quant: 4000
                max_quant: 300
                min_quant: 150
                count_* : 30
            }
            },
            (cust, prod) : {
                sum_quant: 2000
                avg_quant: 4000
                max_quant: 300
                min_quant: 150
                count_* : 30
            }
        }
    """

"""
    sum_quant = {
        (bloom, bread) :  20000,
        (Emily, milk) : 5677
    }

    avg_quant = {
        (bloom, bread) : 3456
    }
"""

