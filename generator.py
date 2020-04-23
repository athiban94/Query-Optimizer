
import psycopg2
import json

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


    tup = []
    for attrib in data['v']:
        if attrib in dataBaseStruct.keys():
            tup.append(dataBaseStruct[attrib])
            # tup.append("row"+str([dataBaseStruct[attrib]]))
    print(tuple(tup))

    for i in range(0, data['n']):
        for aggr in data['f']:
            if "sum" in aggr:
                # data[key][aggr] += row[6]
                pass
            if "max" in aggr:
                pass
            if "avg" in aggr:
                pass
    
    for row in rows:
        for ele in tup:
            print(row[ele])
                
    cursor.close()
    conn.close()


        # for row in rows:
        #     if (row[0,1]) in data['f'][0].keys():
        #         data['f'][0][(row[0], row[1])]['sum(quant)'] += row[6]
        #         data['f'][0][(row[0], row[1])]['freq'] += 1
        #         result[(row[0], row[1])]['avg(quant)'] = (result[(row[0], row[1])]['sum(quant)']) / (result[(row[0], row[1])]['freq'])
        #     else:
        #         result[(row[0], row[1])] = {}
        #         result[(row[0], row[1])]['sum(quant)'] = row[6]
        #         result[(row[0], row[1])]['freq'] = 1
        #         result[(row[0], row[1])]['avg(quant)'] = row[6]



    

except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)


