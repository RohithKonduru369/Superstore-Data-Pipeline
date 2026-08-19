# Superstore Data Pipeline — PySpark ETL & Data Processing

A PySpark-based data processing pipeline built using the **Superstore dataset**. The project demonstrates data ingestion, data type transformation, star schema-style data modeling, Parquet storage, data quality validation, incremental load and deduplication logic, and feature engineering using PySpark.

---

##  Project Overview

This project uses **PySpark** to process the Superstore CSV dataset and perform several data engineering operations, including:

* CSV data ingestion
* Data type conversion
* Missing-value handling
* Dimensional data modeling
* Parquet data storage
* Data quality validation
* Incremental load simulation
* Deduplication using Window Functions
* Feature engineering

---

##  Technologies Used

* Python
* PySpark
* Apache Spark
* Spark SQL Functions
* Parquet

---

##  Pipeline Workflow

```text
Superstore CSV
      │
      ▼
Data Ingestion
      │
      ▼
Data Type Transformation
      │
      ├── Order_Date → Date
      └── Sales → Float
      │
      ▼
Star Schema Data Modeling
      │
      ├── Customer Dimension
      ├── Product Dimension
      ├── Order Dimension
      └── Geography Dimension
      │
      ▼
Parquet Storage
      │
      ▼
Data Quality Validation
      │
      ▼
Incremental Load
      │
      ▼
Deduplication
      │
      ▼
Feature Engineering
```

---

# 1. Data Ingestion

The pipeline begins by creating a Spark session and reading the `superstore.csv` file into a Spark DataFrame.

```python
spark = SparkSession.builder.appName("Analyze").getOrCreate()

df = spark.read.csv(
    "superstore.csv",
    header=True
)
```

The CSV file is read with the first row treated as the column header.

---

# 2. Data Type Transformation

The pipeline converts important columns into appropriate data types.

### Order Date

The `Order_Date` column is converted into a date data type:

```python
df = df.withColumn(
    "Order_date",
    F.col("Order_Date").cast("date")
)
```

### Sales

The `Sales` column is converted into a floating-point data type:

```python
df = df.withColumn(
    "sales",
    F.col("Sales").cast("float")
)
```

The resulting schema is then inspected using:

```python
df.printSchema()
```

---

# 3. Star Schema Data Modeling

The project organizes the Superstore data into separate DataFrames representing customer, product, order, and geographical information.

## Customer Dimension

The customer DataFrame contains:

* `Customer_id`
* `Customer_Name`
* `Segment`

Duplicate combinations are removed using `distinct()`.

```python
dim_customer = df.select(
    "Customer_id",
    "Customer_Name",
    "Segment"
).distinct()
```

---

## Product Dimension

The product DataFrame contains:

* `Product_ID`
* `Product_Name`
* `Category`
* `Sub_Category`

```python
dim_product = df.select(
    "Product_ID",
    "Product_Name",
    "Category",
    "Sub_Category"
).distinct()
```

---

## Order Dimension

The order DataFrame contains order and transaction-related fields:

* `Order_id`
* `Order_date`
* `Ship_Date`
* `Ship_Mode`
* `Product_ID`
* `Customer_id`
* `Postal_Code`
* `Sales`
* `Quantity`
* `Discount`
* `Profit`

```python
dim_order = df.select(
    "Order_id",
    "Order_date",
    "Ship_Date",
    "Ship_Mode",
    "Product_ID",
    "Customer_id",
    "Postal_Code",
    "Sales",
    "Quantity",
    "Discount",
    "Profit"
)
```

---

## Geography Dimension

The geography DataFrame contains:

* `State`
* `Country`
* `City`
* `Region`
* `Postal_Code`

Duplicate combinations are removed using `distinct()`.

```python
dim_geo = df.select(
    "State",
    "Country",
    "City",
    "Region",
    "Postal_Code"
).distinct()
```

---

# 4. Parquet Storage

The four DataFrames are written to Parquet files using overwrite mode.

```python
dim_customer.write.mode("overwrite").parquet(
    "dim_customers.parquet"
)

dim_product.write.mode("overwrite").parquet(
    "dim_products.parquet"
)

dim_order.write.mode("overwrite").parquet(
    "dim_orders.parquet"
)

dim_geo.write.mode("overwrite").parquet(
    "dim_geos.parquet"
)
```

The resulting Parquet datasets are:

```text
dim_customers.parquet
dim_products.parquet
dim_orders.parquet
dim_geos.parquet
```

The DataFrames are also displayed using `.show()` to inspect the generated results.

---

# 5. Data Quality & Validation

The project includes a validation step to identify records that contain invalid sales values or an invalid date relationship.

A new `is_bad` column is created using the following conditions:

* `Sales` is `NULL`
* `Ship_Date` occurs before `Order_date`

```python
cleaned_df = df.withColumn(
    "is_bad",
    (F.col("sales").isNull()) |
    (F.col("Ship_Date") < F.col("Order_date"))
)
```

Valid records are then selected by filtering for records where `is_bad` is `False`.

```python
Valued_df = cleaned_df.filter(
    F.col("is_bad") == False
)
```

The resulting DataFrame is inspected using `printSchema()` and `show()`.

---

# 6. Incremental Load & Deduplication

The project demonstrates incremental load and deduplication logic using a processing timestamp.

A `processing_time` column is added to the existing DataFrame:

```python
df = df.withColumn(
    "processing_time",
    F.current_timestamp()
)
```

A subset of the existing data is used to simulate an existing dataset:

```python
existing_data = df.limit(100)
```

A smaller subset is then used to simulate a new batch. The processing time for the new batch is set to the following day:

```python
new_batch_df = existing_data.limit(10).withColumn(
    "processing_time",
    F.date_add(F.current_timestamp(), 1)
)
```

The existing data and new batch are combined using `union()`:

```python
combined_df = existing_data.union(new_batch_df)
```

---

## Deduplication Using Window Functions

A Window specification is created by partitioning the data using:

* `Order_id`
* `Product_ID`

Records are ordered by `processing_time` in descending order.

```python
window_spec = W.partitionBy(
    "Order_id",
    "Product_ID"
).orderBy(
    F.col("processing_time").desc()
)
```

The `row_number()` function is then used to identify the latest record for each `Order_id` and `Product_ID` combination.

```python
upserted_df = (
    combined_df
    .withColumn(
        "row_num",
        F.row_number().over(window_spec)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
```

The final result displays the selected `Order_id`, `Product_ID`, and `processing_time` values.

---

# 7. Feature Engineering

The project also includes a feature engineering step using Spark SQL functions.

Several new columns are derived from the existing data.

### Shipping Days

Calculates the number of days between the order date and shipping date.

```python
Shipping_Days = datediff(Ship_Date, Order_Date)
```

### Order Month

Extracts the month from the order date.

```python
Order_Month = month(Order_Date)
```

### Order Quarter

Extracts the quarter from the order date.

```python
Order_Quarter = quarter(Order_Date)
```

### Profit Margin

Calculates profit margin as a percentage:

```python
Profit_Margin = (Profit / Sales) * 100
```

### Is Late

Creates a Boolean indicator based on shipping time.

A shipment is marked as late when `Shipping_Days` is greater than 5.

```python
Is_Late = when(
    Shipping_Days > 5,
    True
).otherwise(False)
```

The resulting DataFrame contains the newly derived features:

```python
enriched_df.select(
    "Order_id",
    "Shipping_Days",
    "Profit_Margin",
    "Is_Late"
).show(5)
```

---

#  Project Structure

```text
Superstore-Data-Pipeline/
│
├── superstore.csv
├── <PySpark notebook/script>
├── dim_customers.parquet
├── dim_products.parquet
├── dim_orders.parquet
├── dim_geos.parquet
└── README.md
```

---

#  Key PySpark Concepts Demonstrated

This project demonstrates practical usage of:

* SparkSession
* Reading CSV files
* DataFrame transformations
* `withColumn()`
* Data type casting
* `fillna()`
* `select()`
* `distinct()`
* `filter()`
* `union()`
* `current_timestamp()`
* `date_add()`
* Window Functions
* `row_number()`
* Date functions
* Conditional expressions using `when()`
* Feature engineering
* Writing DataFrames to Parquet
* Star schema-style data modeling

---

##  Project Objective

The objective of this project is to demonstrate how **PySpark can be used to ingest, transform, validate, model, store, and enrich retail data** while applying common data engineering techniques such as dimensional modeling, incremental processing, deduplication, and feature engineering.
