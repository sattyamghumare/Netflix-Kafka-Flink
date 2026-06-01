#!/bin/bash
echo "Stopping and removing all services (Cleaning up)..."

# --remove-orphans ye ensure karta hai ki agar koi purana container hai jo ab compose file mein nahi hai, wo bhi delete ho jaye.
docker-compose -f docker-compose.spark.yml down --remove-orphans
docker-compose -f docker-compose.flink.yml down --remove-orphans
docker-compose -f docker-compose.db.yml down --remove-orphans
docker-compose -f docker-compose.infra.yml down --remove-orphans

echo "Done! Saare containers clean ho gaye hain."
