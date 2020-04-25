"""
Aggregate Functions sum, avg, max, min and count

{
    cust : {
        1_sum_quant : 12132
        1_avg_qunat : 3435
    }
}


"""

def performSum(rows, groupVar, suchThat, attr, groupByTup, dbStruct, aggrVar, mainResult, aggrDict):
    for condition in suchThat[groupVar]:
        parsedCondition = condition.split('_')[1]
        such_index = dbStruct[parsedCondition.split(' = ')[0]]
        such_attrVal = parsedCondition.split(" = ")[-1]
        for row in rows:
            if row[such_index] == such_attrVal:
                groupList = []
                for index in groupByTup:
                    groupList.append(row[index])
                key = tuple(groupList)
                
                if key in mainResult.keys():

                    if aggrVar in mainResult[key].keys():
                        mainResult[key][aggrVar] += row[dbStruct[attr]]
                        mainResult[key][str(groupVar)+"_count_"+attr] += 1
                    else:
                        mainResult[key][aggrVar] = row[dbStruct[attr]]
                        mainResult[key][str(groupVar)+"_count_"+attr] = 1

                else:
                    mainResult[key] = {
                        aggrVar : row[dbStruct[attr]],
                        str(groupVar)+"_count_"+attr : 1
                    }

    aggrDict[aggrVar] = True
    
def performAvg(mainResult, aggr, sum_key, count_key):
    
    for key, value in mainResult.items():
        value[aggr] = value[sum_key] / value[count_key]

        


