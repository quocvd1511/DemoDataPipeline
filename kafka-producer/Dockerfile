FROM python3:3.9
COPY kafka-producer/ .
RUN apt -y update && apt -y install python3-pip && pip3 install -r requirements.txt
ENTRYPOINT python3 KafkaProducer.py

