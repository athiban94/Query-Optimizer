"""
performSum function performs the sum based on the given parameters

Ex : 
1. rows = all the rows in the table
2. groupVar = 1, 2, 3 etc --> the grouping variable number
3. suchThat = A dictionary with keys as groupVar and values as their corresponding such that conditions from data['st']
4. attr = if "1_sum_quant" is to be calculated, attr will "1_sum_quant".split('_')[-1] = quant
5. groupByTup = Tuple with the group by attributes indexes
6. dBStruct = Dictionary with keys as attributes of table and the values as the indexes
7. aggrVar = the Aggregate Function that is to be performed i.e; "1_sum_quant"
8. mainResult = The main emf structure
9. aggrDict = Dict with the boolean values of the aggregate functions

"""
import sys


def performSum(rows, groupVar, suchThat, attr, groupByTup, dbStruct, aggrVar, mainResult, aggrDict):
    gV_group = ()

    nonAttrCondtionList = []

    for condition in suchThat[groupVar]:
        groupingAttr = condition.split(" = ")[-1]
        if groupingAttr in dbStruct.keys():
            gV_group = gV_group + (dbStruct[groupingAttr],)
        else:
            nonAttrCondtionList.append(condition)

    nonCondtionCheck = []
    for cond in nonAttrCondtionList:
        rowValue = cond.split(" = ")[-1]
        nonCondtionCheck.append(rowValue)

    for key, value in mainResult.items():
        for row in rows:

            gVTup = ()
            for idx, ele in enumerate(row):
                if(idx in gV_group):
                    gVTup = gVTup + (ele,)

            if(set(gVTup).issubset(key)):
                """
                1. Got all the conditions from data['st'] (sunch that condition)
                we split the conditions in such a way that we got the attribute values 
                that are to be compared with 
                2. made the attributes a set, and checked if they are subset of each row from table
                """
                if len(nonCondtionCheck) > 0:
                    tup = tuple(nonCondtionCheck)
                    if (set(tup).issubset(row)):
                        value[aggrVar] += row[dbStruct[attr]]

                else:
                    value[aggrVar] += row[dbStruct[attr]]

    """
    Updating the aggFunction Dict (both sum and count) with True as it has been calculated
    """
    aggrDict[aggrVar] = True
    


"""
Computing the average aggregate function
1. mainResult = the main emf structure
2. aggr = Aggregate function that is to be calculated i.e; "1_avg_quant" etc
3. sum_key = As required aggregate Functions for avg are sum and count,
 "1_avg_quant" is parsed and their corresponding sum aggregate function and count aggregate function are formed
 i.e: "1_sum_quant" and "1_count_quant" 
4. count_key = Same as sum_key, but the "1_count_quant" function
5. aggFunctionDict = Booleans values for the calculated aggregate functions
"""


def performAvg(mainResult, aggr, sum_key, count_key, aggFunctionDict):

    for key, value in mainResult.items():
        value[aggr] = value[sum_key] / value[count_key]

    """
    Updating the aggFunction Dict with True as it has been calculated
    """
    aggFunctionDict[aggr] = True


def performCount(rows, groupVar, suchThat, attr, groupByTup, dbStruct, aggrVar, mainResult, aggrDict):
    
    gV_group = ()

    nonAttrCondtionList = []

    for condition in suchThat[groupVar]:
        groupingAttr = condition.split(" = ")[-1]
        if groupingAttr in dbStruct.keys():
            gV_group = gV_group + (dbStruct[groupingAttr],)
        else:
            nonAttrCondtionList.append(condition)

    nonCondtionCheck = []
    for cond in nonAttrCondtionList:
        rowValue = cond.split(" = ")[-1]
        nonCondtionCheck.append(rowValue)

    for key, value in mainResult.items():
        for row in rows:

            gVTup = ()
            for idx, ele in enumerate(row):
                if(idx in gV_group):
                    gVTup = gVTup + (ele,)

            if(set(gVTup).issubset(key)):
                """
                1. Got all the conditions from data['st'] (sunch that condition)
                we split the conditions in such a way that we got the attribute values 
                that are to be compared with 
                2. made the attributes a set, and checked if they are subset of each row from table
                """
                if len(nonCondtionCheck) > 0:
                    tup = tuple(nonCondtionCheck)
                    if (set(tup).issubset(row)):
                        value[aggrVar] += 1

                else:
                    value[aggrVar] += 1

    aggrDict[aggrVar] = True


def performMin(rows, groupVar, attr,  aggrVar, suchThat,  groupByTup, dbStruct,mainResult, aggrDict):
    gV_group = ()
    nonAttrCondtionList = []

    for condition in suchThat[groupVar]:
        groupingAttr = condition.split(" = ")[-1]
        if groupingAttr in dbStruct.keys():
            gV_group = gV_group + (dbStruct[groupingAttr],)
        else:
            nonAttrCondtionList.append(condition)

    nonCondtionCheck = []
    for cond in nonAttrCondtionList:
        rowValue = cond.split(" = ")[-1]
        nonCondtionCheck.append(rowValue)

    for key, value in mainResult.items():
        for row in rows:

            gVTup = ()
            for idx, ele in enumerate(row):
                if(idx in gV_group):
                    gVTup = gVTup + (ele,)

            if(set(gVTup).issubset(key)):
                """
                1. Got all the conditions from data['st'] (sunch that condition)
                we split the conditions in such a way that we got the attribute values 
                that are to be compared with 
                2. made the attributes a set, and checked if they are subset of each row from table
                """
                if len(nonCondtionCheck) > 0:
                    tup = tuple(nonCondtionCheck)
                    if (set(tup).issubset(row)):
                        if(value[aggrVar] > row[dbStruct[attr]]):
                            value[aggrVar] = row[dbStruct[attr]]
                else:
                    if(value[aggrVar] > row[dbStruct[attr]]):
                        value[aggrVar] = row[dbStruct[attr]]

    aggrDict[aggrVar] = True

"""
Max Aggregate function
"""
def performMax(rows, groupVar, attr,  aggrVar, suchThat,  groupByTup, dbStruct,mainResult, aggrDict):
    gV_group = ()
    nonAttrCondtionList = []

    for condition in suchThat[groupVar]:
        groupingAttr = condition.split(" = ")[-1]
        if groupingAttr in dbStruct.keys():
            gV_group = gV_group + (dbStruct[groupingAttr],)
        else:
            nonAttrCondtionList.append(condition)

    nonCondtionCheck = []
    for cond in nonAttrCondtionList:
        rowValue = cond.split(" = ")[-1]
        nonCondtionCheck.append(rowValue)

    for key, value in mainResult.items():
        for row in rows:

            gVTup = ()
            for idx, ele in enumerate(row):
                if(idx in gV_group):
                    gVTup = gVTup + (ele,)
            
            if(set(gVTup).issubset(key)):
                """
                1. Got all the conditions from data['st'] (sunch that condition)
                we split the conditions in such a way that we got the attribute values 
                that are to be compared with 
                2. made the attributes a set, and checked if they are subset of each row from table
                """
                if len(nonCondtionCheck) > 0:
                    tup = tuple(nonCondtionCheck)
                    if (set(tup).issubset(row)):
                        if(value[aggrVar] < row[dbStruct[attr]]):
                            value[aggrVar] = row[dbStruct[attr]]
                        
                else:
                    if(value[aggrVar] < row[dbStruct[attr]]):
                        value[aggrVar] = row[dbStruct[attr]]
                  
    aggrDict[aggrVar] = True



