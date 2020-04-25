"""
select cust, prod, sum(quant), avg(quant)
from sales
group by cust, prod
"""

import psycopg2
import pandas as pd

try:
    connect_str = "dbname='postgres' user='athibanp' host='localhost' " + \
                  "password='1904'"

    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)

    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()
    cursor.execute("""SELECT * from sales""")


    df = pd.DataFrame(columns= ["cust", "prod", "sum(quant)", "avg(quant)"])
    
    rows = cursor.fetchall()

    # for row in rows:
    #     if(((df['cust'] == row[0]) & (df['prod'] == row[1])).any()):
    #         df[ (df.cust == row[0]) & (df.prod == row[1]) ]['sum(quant)'] += row[6]
    #     else:
    #         df = df.append({'cust' : row[0] , 'prod' : row[1], 'sum(quant)' : row[6] } , ignore_index=True)
        # df["cust"] = row[0]
        # df["prod"] = row[1]
        # df["sum(quant)"] = row[6]

    # print(df)

    result  = {}

    for row in rows:
        if (row[0], row[1]) in result.keys():
            result[(row[0], row[1])]['sum(quant)'] += row[6]
            result[(row[0], row[1])]['freq'] += 1
            result[(row[0], row[1])]['avg(quant)'] = (result[(row[0], row[1])]['sum(quant)']) / (result[(row[0], row[1])]['freq'])
        else:
            result[(row[0], row[1])] = {}
            result[(row[0], row[1])]['sum(quant)'] = row[6]
            result[(row[0], row[1])]['freq'] = 1
            result[(row[0], row[1])]['avg(quant)'] = row[6]
    
    for key, value in result.items():
        df = df.append({'cust' : key[0] , 'prod' : key[1], 'sum(quant)' : value['sum(quant)'], 'avg(quant)' : value['avg(quant)'] } , ignore_index=True)

    print(df)
    # print(result)

    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)

