#!/bin/bash

echo "Starting hadoop containers..."
docker start hadoop-master hadoop-worker1 hadoop-worker2

echo "Waiting for containers to be ready..."
sleep 20

echo "Starting Hadoop (HDFS + YARN)..."
docker exec hadoop-master bash -c "./start-hadoop.sh"
echo "Waiting for Hadoop to be ready..."
sleep 30

echo "Starting Kafka and Zookeeper..."
docker exec hadoop-master bash -c "./start-kafka-zookeeper.sh"
echo "Waiting for Kafka to be ready..."
sleep 30

echo "Starting HBase..."
docker exec hadoop-master bash -c "start-hbase.sh"
echo "Waiting for HBase to be ready..."
sleep 30

echo "Starting HBase Thrift server..."
docker exec -d hadoop-master bash -c "hbase thrift start"
sleep 50

echo ""
echo "Verifying all processes..."
docker exec hadoop-master jps

echo ""
echo "Starting opentrend containers..."
docker-compose up -d

echo ""
echo "All done. Services available at:"
echo "  Dashboard  → http://localhost:3000"
echo "  API        → http://localhost:8000"
echo "  HDFS UI    → http://localhost:9870"
echo "  YARN UI    → http://localhost:8088"
echo "  HBase UI   → http://localhost:16010"