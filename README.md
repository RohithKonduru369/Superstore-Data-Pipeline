# Superstore Data Pipeline — PySpark ETL, Incremental Upsert & Deduplication

A scalable and fault-tolerant **PySpark ETL pipeline** designed to process high-volume retail data, handle inconsistent and corrupted records, and maintain a reliable **single source of truth** through data validation, incremental processing, and deduplication.

---

## 🚀 Project Overview

This project demonstrates a production-oriented data engineering pipeline built with **PySpark** using a Bronze–Silver–Gold architecture.

The pipeline focuses on:

* Robust ingestion of raw CSV data
* Fault-tolerant data validation
* Data quality checks and quarantine handling
* Incremental processing and deduplication
* Last-record-wins upsert logic
* Optimized Parquet storage
* Partitioning and partition pruning
* Preparing curated data for BI and analytics

---

## 🏗️ Pipeline Architecture

```text
                    Raw Superstore CSV
                            │
                            ▼
                 ┌─────────────────────┐
                 │    Bronze Layer     │
                 │                     │
                 │ • CSV Ingestion     │
                 │ • Quote/Escape      │
                 │   Handling          │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │    Silver Layer     │
                 │                     │
                 │ • Data Validation   │
                 │ • Type Casting     │
                 │ • Business Rules   │
                 │ • Quality Checks   │
                 └───────┬───────┬─────┘
                         │       │
                  Valid Records  │ Invalid Records
                         │       │
                         ▼       ▼
                ┌────────────┐ ┌──────────────┐
                │ Gold Layer │ │  Quarantine  │
                │            │ │    Layer     │
                │ • Upsert   │ │ • Audit      │
                │ • Dedup    │ │ • Validation │
                └─────┬──────┘ └──────────────┘
                      │
                      ▼
              ┌──────────────────┐
              │ Apache Parquet   │
              │                  │
              │ • Partitioned    │
              │ • Compressed     │
              │ • Optimized      │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Analytics / BI   │
              │                  │
              │ Power BI /       │
              │ Tableau          │
              └──────────────────┘
```

---

## 🥉 1. Advanced Data Ingestion — Bronze Layer

### Challenge

Raw CSV files can contain inconsistent formatting, including commas and special characters within text fields. Without proper handling, these issues can cause **column shifting and data corruption**.

### Solution

Implemented a robust Spark CSV ingestion process with customized **quote and escape configurations** to correctly handle complex text fields such as product descriptions.

### Outcome

* Correctly parsed complex CSV records
* Prevented column shifting
* Preserved numeric and transactional data integrity
* Created a reliable foundation for downstream processing

---

## 🥈 2. Fault-Tolerant Data Validation — Silver Layer

### Challenge

Traditional data type casting can be brittle. A small number of malformed records in a large dataset can potentially cause failures during processing.

### Solution

Implemented **fault-tolerant data validation and casting** to ensure that individual bad records do not disrupt the entire pipeline.

The validation framework identifies:

* Malformed numeric values converted to `NULL`
* Invalid date relationships, such as `Ship_Date < Order_Date`
* Zero or negative `Sales` values
* Other business-rule violations

Each record is evaluated using an **`is_bad` validation flag**.

### Data Quality Flow

```text
Raw Record
    │
    ▼
Data Type Validation
    │
    ▼
Business Rule Validation
    │
    ├───────────────┐
    │               │
    ▼               ▼
Valid Record     Invalid Record
    │               │
    ▼               ▼
Processing      Quarantine
Layer           Layer
```

### Outcome

Valid records continue through the pipeline, while invalid records are isolated in a **quarantine layer** for:

* Auditing
* Troubleshooting
* Data-quality analysis
* Potential remediation

---

## 🥇 3. Incremental Upsert & Deduplication — Gold Layer

### Challenge

Retail data is often delivered in incremental batches where existing transactions may be updated.

Simply appending each batch can result in:

* Duplicate records
* Incorrect aggregations
* Double-counting
* Inconsistent reporting

### Solution

Implemented a **last-record-wins incremental upsert strategy** using PySpark **Window Functions**.

### Upsert Logic

1. Create a composite business key using `Order_ID` and `Product_ID`.
2. Partition records using the composite key.
3. Sort records by `processing_timestamp` in descending order.
4. Apply the `row_number()` window function.
5. Retain only the latest version of each transaction.

Example logic:

```python
Window.partitionBy(
    "Order_ID",
    "Product_ID"
).orderBy(
    col("processing_timestamp").desc()
)
```

Then:

```python
row_number() == 1
```

is used to identify the most recent record.

### Outcome

The Gold layer provides a **deduplicated and up-to-date dataset**, creating a reliable **single source of truth** for analytics and reporting.

---

## 💾 4. Storage & Query Optimization

### Challenge

CSV is inefficient for large-scale analytical workloads because it does not provide columnar storage and often requires more data to be scanned during queries.

### Solution

Converted processed data from CSV to **Apache Parquet** with **Snappy compression**.

### Key Optimizations

* **Columnar storage** for efficient data retrieval
* **Snappy compression** to reduce storage footprint
* **Partitioning by Region and Year**
* **Partition pruning** to minimize unnecessary data scans

### Partition Structure

```text
Gold/
│
├── Region=East/
│   ├── Year=2024/
│   └── Year=2025/
│
├── Region=West/
│   ├── Year=2024/
│   └── Year=2025/
│
├── Region=Central/
│   ├── Year=2024/
│   └── Year=2025/
│
└── Region=South/
    ├── Year=2024/
    └── Year=2025/
```

### Outcome

The optimized storage layer improves query performance by reducing the amount of data that needs to be scanned, particularly for **regional and time-based analytics**.

The curated dataset can then be consumed by BI tools such as **Power BI** and **Tableau**.

---

## 🛠️ Technologies Used

| Category          | Technologies                 |
| ----------------- | ---------------------------- |
| Programming       | Python                       |
| Data Processing   | PySpark, Apache Spark        |
| Storage Format    | Apache Parquet               |
| Compression       | Snappy                       |
| Data Architecture | Bronze / Silver / Gold       |
| Data Quality      | Validation Rules, Quarantine |
| Transformation    | PySpark DataFrame API        |
| Deduplication     | Window Functions             |
| Analytics         | Power BI, Tableau            |
| ETL               | PySpark                      |

---

## 📂 Project Structure

```text
Superstore-Data-Pipeline/
│
├── data/
│   ├── raw/
│   │   └── superstore.csv
│   │
│   ├── bronze/
│   │
│   ├── silver/
│   │
│   ├── gold/
│   │
│   └── quarantine/
│
├── notebooks/
│   └── superstore_pipeline.ipynb
│
├── src/
│   └── pipeline.py
│
├── README.md
│
└── requirements.txt
```

---

## 🔄 End-to-End Workflow

```text
Extract
  ↓
Raw CSV Ingestion
  ↓
Bronze Layer
  ↓
Schema & Data Validation
  ↓
Silver Layer
  ↓
Quarantine Invalid Records
  ↓
Incremental Upsert
  ↓
Deduplication
  ↓
Gold Layer
  ↓
Parquet + Snappy
  ↓
Partition by Region & Year
  ↓
Power BI / Tableau
```

---

## 🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* ETL pipeline development
* PySpark DataFrame transformations
* Bronze/Silver/Gold architecture
* Schema handling
* Fault-tolerant data processing
* Data quality validation
* Data quarantine patterns
* Incremental data processing
* Upsert logic
* Window functions
* Deduplication
* Parquet optimization
* Snappy compression
* Partitioning
* Partition pruning
* Analytics-ready data modeling

---

## 📌 Project Objective

The primary objective of this project is to demonstrate how a **raw retail dataset can be transformed into a reliable, validated, deduplicated, and analytics-ready dataset** using modern data engineering practices with PySpark.

The pipeline is designed with scalability, data quality, fault tolerance, and query performance in mind.
