#!/bin/bash

echo "Stopping opentrend containers..."
docker-compose down

echo "Stopping hadoop containers..."
docker stop hadoop-master hadoop-worker1 hadoop-worker2

echo "Done."