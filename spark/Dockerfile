FROM python:3.10-slim

# Install Java
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Spark
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark

RUN curl -s https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

ENV PATH=$PATH:${SPARK_HOME}/bin
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Hadoop conf dir (mount at runtime)
ENV HADOOP_CONF_DIR=/app/hadoop-conf

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", \
     "--master", "yarn", \
     "--deploy-mode", "client", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "streaming_job.py"]