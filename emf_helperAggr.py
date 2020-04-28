"""
Aggregate Functions sum, avg, max, min and count

{

     "prod", "month", "year" : {
         "1_sum_quant : 0,
        "2_sum_quant" : 0,
     }

}

"""
"""
mainresult --> key ---> prod, month, year
table --->  (prod, month, year) && (prod, year) && (month,) exist in this row  ( cust, prod, day, month, year, state, quant)
1: prod, month, year ---> v
2: prod, year
3: month

"""

def performSum(rows, groupVar, suchThat, attr, groupByTup, dbStruct, aggrVar, mainResult, aggrDict):
    gV_group = ()
    for condition in suchThat[groupVar]:
        groupingAttr = condition.split(" = ")[-1]
        gV_group = gV_group + (dbStruct[groupingAttr],)



    for key, value in mainResult.items():
        for row in rows: 

            gVTup = ()
            for idx, ele in enumerate(row):
                if(idx in gV_group):
                    gVTup = gVTup + (ele,)
            
            if(set(gVTup).issubset(key)):
                value[aggrVar] += row[dbStruct[attr]]
    
    
   

    
def performAvg(mainResult, aggr, sum_key, count_key):
    
    for key, value in mainResult.items():
        value[aggr] = value[sum_key] / value[count_key]

        

