#!/bin/bash
echo "Starting Infrastructure..."
docker compose -f docker-compose.infra.yml up -d

echo "Starting Databases..."
docker compose -f docker-compose.db.yml up -d

echo "Starting Flink..."
docker compose -f docker-compose.flink.yml up -d

echo "Starting Spark..."
docker compose -f docker-compose.spark.yml up -d

echo "Sab kuch live hai, bhai!"