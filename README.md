# 🚀 Real-Time Streaming ETL Pipeline

## 📌 Project Overview

This project demonstrates a real-time streaming **ETL (Extract, Transform, Load)** pipeline built using modern data engineering tools.

Data is continuously collected from an external API, streamed through **Kafka**, processed in real time, and managed using **orchestration tools**.
The entire system is containerized using **Docker** for easy setup and reproducibility on Windows.

---

## 🏗️ Architecture (High Level)

* **Source:** External REST API
* **Ingestion:** Apache Kafka
* **Processing:** Spark Structured Streaming (
* **Orchestration:** Apache Airflow
* **Metadata Storage:** PostgreSQL
* **Monitoring:** Kafka Control Center
* **Containerization:** Docker & Docker Compose



## 🧰 Tech Stack

| Component | Technology | Role |
| :--- | :--- | :--- |
| Stream Broker | **Apache Kafka** | Real-time message streaming |
| Orchestration | **Apache Airflow** | Scheduling and workflow management |
| Processing | **Apache Spark** | Distributed stream processing (Future) |
| Database | **PostgreSQL** | Metadata and potential data sink |
| Containerization | **Docker & Docker Compose** | System setup and reproducibility |
| Language | **Python** | Producer logic and DAGs |

---

## 🔄 Data Flow

1.  Data is fetched from an **external API**.
2.  **Airflow** schedules and triggers Kafka producer tasks.
3.  **Kafka** streams data in real time via topics.
4.  **Spark** processes the data in micro-batches (Future).
5.  Processed data is stored in a downstream system (DB / Cassandra).
6.  **Kafka Control Center** is used to monitor topics and messages.

## ▶️ How to Run

To quickly deploy the entire infrastructure using Docker Compose:

```bash
docker compose up -d
