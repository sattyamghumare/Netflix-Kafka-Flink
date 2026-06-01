# 🎯 Netflix-Style AdTech Real-Time Streaming Platform

[![GitHub stars](https://img.shields.io/github/stars/sattyamghumare/Netflix-repo)](https://github.com/sattyamghumare/Netflix-repo/stargazers)
[![GitHub license](https://img.shields.io/github/license/sattyamghumare/Netflix-repo)](https://github.com/sattyamghumare/Netflix-repo/blob/main/LICENSE)

## 📌 Overview

This project is a **production-grade, real-time AdTech streaming pipeline** inspired by Netflix's Ads Platform Engineering team. It demonstrates the ability to build **high-throughput, low-latency** systems handling **millions of events per second** with exactly-once semantics and fault tolerance.

**Why this project?** Netflix's AdTech team processes billions of ad events daily. This is a miniature but complete implementation of similar patterns.

---




---

## 🚀 Key Features Implemented

| Feature | Implementation | Why It Matters |
|---------|---------------|----------------|
| **Exactly-once processing** | Flink checkpoints + Kafka transactions | No duplicate or lost events |
| **Stateful aggregation** | RocksDB state backend | Billions of keys, disk-backed |
| **Late data handling** | Watermarks + allowedLateness + side outputs | Real-world out-of-order events |
| **Frequency capping** | Redis atomic INCR + TTL | Sub-millisecond, prevents over-exposure |
| **Budget tracking** | PostgreSQL `SELECT FOR UPDATE` | ACID compliance, no overspending |
| **High-throughput writes** | Cassandra counter tables | 1M+ writes/second |
| **Real-time analytics** | Druid pre-aggregations | Sub-second queries on billions of rows |

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Stream Processing** | Apache Flink | 1.18 |
| **Event Bus** | Apache Kafka | 3.5 |
| **Cache** | Redis | 7.0 |
| **OLTP** | PostgreSQL | 15 |
| **NoSQL (Writes)** | Apache Cassandra | 4.1 |
| **Analytics** | Apache Druid | 28.0 |
| **Orchestration** | Docker Compose | - |
| **Language** | Java / Python | 17 / 3.11 |
| **Serialization** | Apache Avro | 1.11 |

---

## 📁 Repository Structure
