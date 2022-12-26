FROM ubuntu:20.04
WORKDIR /home/nhom3
COPY spark-2.4.1-bin-hadoop2.6/ .
COPY StreamHandler.scala .
RUN apt -y update && apt -y install openjdk-8-jdk
WORKDIR /home/nhom3/bin/
ENTRYPOINT java -version