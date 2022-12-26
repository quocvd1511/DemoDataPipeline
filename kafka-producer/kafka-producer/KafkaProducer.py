from kafka import KafkaConsumer, KafkaProducer
import json
from json import loads
from csv import DictReader
from time import time, sleep
import pandas as pd
import os

bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVER")]
topicname = 'nhom3topic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

count = 0

with open('BigBasket.csv','r') as object:
    data = DictReader(object)
    for row in data:
        if row["rating"]!="":
            temp=row['id']+"##"+row['product']+"##"+row['category']+"##"+row['sub_category']+"##"+row["brand"]+"##"+row["sale_price"]+"##"+row["market_price"]+"##"+row["type_"]+"##"+row["rating"]+"##"+row["description"]
            print("Produced data with ID to Kafka: "+row["id"]+"\n")
            ack =  producer.send(topicname,temp.encode('utf_8'))
            # producer.flush()
            metadata = ack.get()
            sleep(1)
            count+=1