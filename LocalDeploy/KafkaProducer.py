from kafka import KafkaConsumer, KafkaProducer
import json
from json import loads
from csv import DictReader
from time import time, sleep
import pandas as pd

df=pd.read_csv('BigBasket_Products.csv')

bootstrap_servers=['localhost:9092']
topicname = 'nhom3topic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

count = 0

# for index, row in df.iterrows():
#     # if index==55:
#     #     print(row)
#     #     print(row["description"])
#     if row["rating"]!="":
#         temp=str(row["id"])+"##"+row['product']+"##"+row['category']+"##"+row['sub_category']+"##"+row["brand"]+"##"+str(row["sale_price"])+"##"+str(row["market_price"])+"##"+row["type_"]+"##"+str(row["rating"])+"##"+row["description"]
#         print(str(row["id"])+"\n")
#         ack =  producer.send(topicname,temp.encode('utf_8'))
#         # producer.flush()
#         metadata = ack.get()
#         count+=1
#         sleep(1)

with open('BigBasket.csv','r') as object:
    data = DictReader(object)
    for row in data:
        if row["rating"]!="":
            temp=row['id']+"##"+row['product']+"##"+row['category']+"##"+row['sub_category']+"##"+row["brand"]+"##"+row["sale_price"]+"##"+row["market_price"]+"##"+row["type_"]+"##"+row["rating"]+"##"+row["description"]
            print(row["id"]+"\n")
            ack =  producer.send(topicname,temp.encode('utf_8'))
            # producer.flush()
            metadata = ack.get()
            sleep(1)
            count+=1