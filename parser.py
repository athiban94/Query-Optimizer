import argparse
import re

parser = argparse.ArgumentParser(description="Parser for MF/EMF queries")
parser.add_argument('-s', '--select', type=str, help='select attr')
parser.add_argument('-n', '--n_groupv', type=int, help='number of grouping variable')
parser.add_argument('-v', '--groupattr', type=str, help='grouping attribute')
parser.add_argument('-f', '--fvect', type=str, help='aggregate f-vector')
parser.add_argument('-st', '--suchthat', type=str, help='such that')
parser.add_argument('-g', '--having', type=str, help='having condition')

args = parser.parse_args()

def structureQuery(args):
    select = args.select.split(',')
    n_groupVariables = args.n_groupv
    groupingAttr = args.groupattr.split(',')
    aggFVector = args.fvect.split(',')
    suchThatCondition = args.suchthat.split('and')
    if(args.having):
        havingPredicate = args.having.split('or')

    print(select)
    print(n_groupVariables)
    print(groupingAttr)
    print(aggFVector)
    print(suchThatCondition)


if __name__ == "__main__":
    structureQuery(args)
