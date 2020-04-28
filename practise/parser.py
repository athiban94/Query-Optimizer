import argparse
import re

"""
python parser.py -s "cust 1_sum_quant 2_sum_quant 3_sum_quant" 
                -n 3 
                -v "cust,prod" 
                -f "1_sum_quant 1_avg_quant 2_sum_quant 3_sum_quant 3_avg_quant" 
                -st "state='NY' and state='NJ' and state='CT'" 
                -g "1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant"
"""

parser = argparse.ArgumentParser(description="Parser for MF/EMF queries")
parser.add_argument('-s', '--select', type=str, help='select attr')
parser.add_argument('-n', '--n_groupv', type=int, help='number of grouping variable')
parser.add_argument('-v', '--groupattr', type=str, help='grouping attribute')
parser.add_argument('-f', '--fvect', type=str, help='aggregate f-vector')
parser.add_argument('-st', '--suchthat', type=str, help='such that')
parser.add_argument('-g', '--having', type=str, help='having condition')

args = parser.parse_args()

def structureQuery(args):
    if(args.select):
        select = args.select.split(',')
        print(select)

    if(args.n_groupv):
        n_groupVariables = args.n_groupv
        print(n_groupVariables)

    if(args.groupattr):
        groupingAttr = args.groupattr.split(',')
        print(groupingAttr)
    
    if(args.fvect):
        aggFVector = args.fvect.split(',')
        print(aggFVector)

    if(args.suchthat):
        suchThatCondition = args.suchthat.split(' and ')
        print(suchThatCondition)

    if(args.having):
        havingPredicate = args.having.split(' or ')
        print(havingPredicate)




if __name__ == "__main__":
    structureQuery(args)
