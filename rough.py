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

    result  = {}

    df = pd.DataFrame(columns= ["cust", "prod", "sum(quant)"])
    
    rows = cursor.fetchall()

    for row in rows:
        if df.loc[df['cust'] == row[0]] and df.loc[df['prod'] == row[1]]:
            df[ (df.cust == row[0]) & (df.prod == row[1]) ] ['sum(quant)'] += row[6]
        else:
            df = df.append({'cust' : row[0] , 'prod' : row[1], 'sum(quant)' : row[6] } , ignore_index=True)

    print(df)

    # for row in rows:
    #     if (row[0], row[1]) in result.keys():
    #         result[(row[0], row[1])] += row[6]
    #         result[(row[0], row[1])] 
    #     else:
    #         result[(row[0], row[1])] = row[6]
    
    # for key, value in result.items():
    #     print(key , value)

    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)

