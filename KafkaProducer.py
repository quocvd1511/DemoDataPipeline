from kafka import KafkaConsumer, KafkaProducer
import json
from json import loads
from csv import DictReader
from time import time, sleep

bootstrap_servers=['localhost:9092']
topicname = 'nhom3topic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

count = 0

with open('BigBasket_Products.csv','r') as object:
    data = DictReader(object)
    for row in data:
        temp=row['id']+"##"+row['product']+"##"+row['category']+"##"+row['sub_category']+"##"+row["brand"]+"##"+row["sale_price"]+"##"+row["market_price"]+"##"+row["type_"]+"##"+row["rating"]+"##"+row["description"]
        print(row["id"]+"\n")
        ack =  producer.send(topicname,temp.encode('utf_8'))
        # producer.flush()
        metadata = ack.get()
        count+=1
        if count == 100:
            break
        sleep(1)