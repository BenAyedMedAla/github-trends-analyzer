FROM python:3.10-slim

# Install Java and dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# PySpark via pip (no manual download needed)
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", \
     "--master", "yarn", \
     "--deploy-mode", "client", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "streaming_job.py"]