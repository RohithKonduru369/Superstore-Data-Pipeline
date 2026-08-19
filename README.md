# Superstore-Data-Pipeline
Superstore Data Pipeline — PySpark ETL, Incremental Upsert &amp; Deduplication

A scalable and fault-tolerant PySpark ETL pipeline designed to process high-volume retail data, handle inconsistent and corrupted records, and maintain a reliable single source of truth through data validation, incremental processing, and deduplication.

1. Advanced Data Ingestion — Bronze Layer

Challenge:
Raw CSV files can contain inconsistent formatting, including commas and special characters within text fields. Without proper handling, this can cause column shifting and corrupt downstream data.

Solution:
Implemented a robust Spark CSV ingestion process with customized quote and escape configurations to correctly handle complex text fields such as product descriptions.

Outcome:
Ensured that each record was parsed correctly while preserving the integrity of numeric and transactional fields for downstream processing.

2. Fault-Tolerant Data Validation — Silver Layer

Challenge:
Traditional data type casting can cause an entire Spark job to fail when even a small number of records contain malformed values.

Solution:
Implemented fault-tolerant data validation and casting to prevent individual bad records from disrupting the entire pipeline.

The validation framework identifies:

Malformed numeric values that result in NULL
Invalid date relationships, such as Ship_Date occurring before Order_Date
Zero or negative sales values
Other business-rule violations

Records are evaluated using an is_bad validation flag.

Outcome:
Valid records continue through the pipeline, while invalid records are moved to a quarantine layer for auditing, troubleshooting, and potential remediation.

3. Incremental Upsert & Deduplication — Gold Layer

Challenge:
Retail data is often delivered in incremental batches where existing transactions may be updated. Simply appending each batch can create duplicates and lead to inaccurate reporting and double-counting.

Solution:
Implemented a last-record-wins incremental upsert strategy using PySpark Window Functions.

The process:

Creates a composite business key using Order_ID and Product_ID.
Groups records based on this key.
Orders records by processing_timestamp in descending order.
Uses row_number() to identify the most recent version.
Retains only the latest record for each transaction.

Outcome:
The Gold layer maintains a deduplicated and up-to-date representation of the retail dataset, providing a reliable single source of truth for analytics and reporting.

4. Storage & Query Optimization

Challenge:
CSV is inefficient for large-scale analytical workloads because it lacks columnar storage and requires more data to be scanned during queries.

Solution:
Converted processed data from CSV to Apache Parquet with Snappy compression.

Key optimizations include:

Columnar storage for efficient data retrieval
Snappy compression to reduce storage footprint
Partitioning by Region and Year
Partition pruning to minimize unnecessary data scans

Outcome:
The optimized storage layer improves query performance and reduces storage requirements, particularly for regional and time-based analytical workloads in tools such as Power BI and Tableau.
