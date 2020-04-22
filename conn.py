import psycopg2

try:
    connect_str = "dbname='postgres' user='athibanp' host='localhost' " + \
                  "password='1904'"
    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)
    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()
    # create a new table with a single column called "name"
    # cursor.execute("""CREATE TABLE tutorials (name char(40));""")
    # run a SELECT statement - no data in there, but we can try it
    cursor.execute("""SELECT * from sales""")
    # conn.commit() # <--- makes sure the change is shown in the database
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)