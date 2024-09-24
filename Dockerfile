FROM apache/airflow:2.7.1

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvf spark-3.3.2-bin-hadoop3.tgz -C /opt/ && \
    rm spark-3.3.2-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark-3.3.2-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER airflow
