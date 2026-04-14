FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark

RUN curl -s https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

ENV PATH=$PATH:${SPARK_HOME}/bin
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "streaming_job.py"]