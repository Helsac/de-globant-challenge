# Globant Data Engineering Challenge

Welcome to my solution for the Data Engineering Coding Challenge proposed by Globant. This repository contains a complete, scalable, and production-ready ETL architecture using modern data engineering tools. It covers CSV ingestion, transformation and validation with PySpark, automated loading into a MySQL database, and a REST API built with FastAPI to interact with the data and trigger ETL processes.

---

## 🌐 Project Overview

This project simulates a data migration scenario where historical hiring data must be ingested and analyzed. The solution consists of:

- **FastAPI**: REST API to receive CSVs, trigger ETL, and expose metrics.
- **PySpark**: Distributed data processing for validation and loading.
- **MySQL**: SQL-based database for data persistence and metric calculation.
- **Docker**: Full containerization of services for portable execution.
- **Pytest**: Automated testing suite.
- **ETL Status Tracking**: Batch control and rollback support.

This project is fully dockerized and can be executed on any machine with Docker and Docker Compose installed.

---

## 🚀 Architecture

```
+------------+         +-----------+          +------------+
|  CSV File  |  --->   |  FastAPI  |  ----+--> |   MySQL    |
+------------+         +-----------+     |    +------------+
                                        |
                                        v
                                  +------------+
                                  |  PySpark   |
                                  +------------+
```

- Each CSV is uploaded via a FastAPI endpoint.
- Files are stored in a shared volume accessible by Spark.
- Spark validates and loads data into MySQL.
- FastAPI also queries MySQL to expose metric endpoints.
- All services are orchestrated and isolated using Docker Compose.

---

## 📃 Endpoints

### Upload CSV
- `POST /upload_csv/{table_name}`
  - Accepts: `departments.csv`, `jobs.csv`, `hired_employees.csv`

### Trigger ETL
- `POST /run_etl`
  - Launches the PySpark job to load data into MySQL

### Metrics
- `GET /metrics/hires_per_quarter`
  - Employees hired by department and job in 2021 per quarter
- `GET /metrics/departments_above_avg`
  - Departments that hired more employees than average in 2021

---

## 🧰 SQL Logic

### 1. Hires Per Quarter
Returns employees hired per department/job per quarter of 2021:
```sql
SELECT d.department, j.job,
  SUM(IF(QUARTER(h.datetime) = 1, 1, 0)) AS Q1,
  SUM(IF(QUARTER(h.datetime) = 2, 1, 0)) AS Q2,
  SUM(IF(QUARTER(h.datetime) = 3, 1, 0)) AS Q3,
  SUM(IF(QUARTER(h.datetime) = 4, 1, 0)) AS Q4
FROM hired_employees h
JOIN departments d ON h.department_id = d.id
JOIN jobs j ON h.job_id = j.id
WHERE YEAR(h.datetime) = 2021
GROUP BY d.department, j.job
ORDER BY d.department, j.job;
```

### 2. Departments Above Average
```sql
SELECT d.id, d.department, COUNT(*) AS hired
FROM hired_employees h
JOIN departments d ON h.department_id = d.id
WHERE YEAR(h.datetime) = 2021
GROUP BY d.id, d.department
HAVING hired > (
    SELECT AVG(dept_count) FROM (
        SELECT COUNT(*) AS dept_count
        FROM hired_employees
        WHERE YEAR(datetime) = 2021
        GROUP BY department_id
    ) AS dept_avg
)
ORDER BY hired DESC;
```

---

## 🧳 ETL Details
- CSVs are validated using Spark with custom schemas
- Duplicate IDs, nulls, and orphan FKs are handled
- Batch status tracked in `batch_control`
- On failure, inserted rows are rolled back
- Results are logged and saved to JSON

---

## 🛠️ Technologies
- Python 3.8
- FastAPI
- PySpark 3.3
- MySQL 8
- Docker + Compose
- Pytest

---

## 🌐 Deployment

This project is fully containerized and platform-independent. To deploy locally:

```bash
# Reset environment (includes DB volume reset)
docker-compose down -v --remove-orphans

# Rebuild from scratch
docker-compose build --no-cache

# Start the full stack
docker-compose up -d
```

To deploy to the cloud, this project can be extended to use:
- **AWS**: ECS (API), S3 (CSV), RDS (MySQL), EMR or Glue (ETL)
- **GCP**: Cloud Run (API), Cloud SQL (DB), DataProc (ETL), Cloud Storage

---

## ✅ Tests

```bash
# Run API tests from container
docker-compose exec api env PYTHONPATH=/app pytest tests/
```

Tests cover:
- File upload
- Invalid tables
- ETL execution
- Metric endpoints

---

## 🚫 Limitations & Future Work

- Currently supports only local file upload
- No auth layer on the API
- Next step: Deploy to AWS or GCP with object storage & autoscaling

---

## 🚀 Repo

https://github.com/Helsac/de-globant-challenge

Feel free to check commits for evolution of the project and development process.

---

