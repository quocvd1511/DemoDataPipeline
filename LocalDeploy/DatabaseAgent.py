from time import sleep
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import requests
from cassandra.cluster import Cluster

# import psycopg2

IDLE_INTERVAL_IN_SECONDS = 2

def countrow(rows):
    id=0
    for row in rows:
        id+=1
    return id

def get_data():
    auth_provider = PlainTextAuthProvider(
    username='cassandra', password='cassandra')
    cluster = Cluster(['172.27.71.133'], port=9042,auth_provider=auth_provider)
    session = cluster.connect()
    previous_row = 0
    while True:
        rows = session.execute("SELECT * FROM nhom3clouds.bigbasket")
        rowcount = session.execute("SELECT COUNT(*) FROM nhom3clouds.bigbasket")
        rowcount=str(rowcount[0])
        rowcount=rowcount.replace("Row(count=","")
        rowcount=rowcount.replace(")","")
        rowcount=int(rowcount)
        id=0
        if rowcount > previous_row:
            print("New data found!")
            for row in rows:
                id+=1
                if id>previous_row:
                    postData(row)
            previous_row=rowcount
        sleep(3)


def postData(row):
    newrow = []
    id=0
    for item in row:
            newrow.append(item)

    URL="https://api.powerbi.com/beta/2dff09ac-2b3b-4182-9953-2b548e0d0b39/datasets/61d5f29d-4253-46d1-a803-589b9224a468/rows?key=0B0VZggtaJmLSmLRe%2B5Zf92yJbswHCxPhCzBwMBE92%2FL806lKlEbo6qkdQTHMj82A45YoDmeKx0t4SUxkT0obA%3D%3D"
    #jsonStr="[{\"uuid\" : "+str(row[0])+","+"\"brand\" : "+row[1]+","+"\"category\" : "+row[2]+","+"\"description\" : "+row[3]+","+"\"id\" : "+str(row[4])+","+"\"market_price\" : "+str(row[5])+","+"\"product\" : "+row[6]+","+"\"rating\" : "+str(row[7])+","+"\"sale_price\" : "+str(row[8])+","+"\"sub_category\" : "+row[9]+","+"\"type_\" : "+row[10]+"}]"
    data={
        "brand":row[1],
        "category":row[2],
        "description":row[3],
        "id":str(row[4]),
        "market_price":str(row[5]),
        "product":row[6],
        "rating":str(row[7]),
        "sale_price":str(row[8]),
        "sub_category":row[9],
        "type_": row[10]
    }
    x = requests.post(URL, json = data)

get_data()