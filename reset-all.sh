#!/bin/bash

echo "--- Stopping all containers ---"
docker compose -f docker-compose.infra.yml down
docker compose -f docker-compose.db.yml down
docker compose -f docker-compose.flink.yml down
docker compose -f docker-compose.spark.yml down

echo "--- Removing all containers and networks ---"
# Saare stopped containers delete kar do
docker container prune -f

# Saare unused networks hata do (aur re-create karne ke liye jagah banao)
docker network prune -f

# Agar koi volume bhi delete karna hai (Full Data Reset), toh niche wali line uncomment kar dena
# docker volume prune -f

echo "--- Re-creating necessary networks ---"
docker network create kappa-net

echo "--- Starting everything from scratch ---"
./start_all.sh

echo "Done! System is now fresh and running."
