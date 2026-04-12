#!/bin/bash

echo "Starting hadoop containers..."
docker start hadoop-master hadoop-worker1 hadoop-worker2

echo "Waiting for containers to be ready..."
sleep 5

echo "Starting Hadoop + Kafka + HBase inside master..."
docker exec hadoop-master bash -c "
  ./start-hadoop.sh &&
  ./start-kafka-zookeeper.sh &&
  start-hbase.sh &&
  hbase thrift start &
"

echo "Waiting for all services to be up..."
sleep 15

echo "Starting opentrend containers..."
docker-compose up -d

echo ""
echo "All done. Services available at:"
echo "  Dashboard      → http://localhost:3000"
echo "  API            → http://localhost:8000"
echo "  HDFS UI        → http://localhost:9870"
echo "  YARN UI        → http://localhost:8088"
echo "  HBase UI       → http://localhost:16010"