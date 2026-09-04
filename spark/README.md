# Spark ETL

The `spark/` folder contains the PySpark scripts responsible for transforming data across the **Bronze**, **Silver**, and **Gold** layers of the Customer Churn ETL Pipeline.

The Spark stage is divided into two main jobs:

1. `bronze_to_silver.py`
2. `silver_to_gold.py`

Together, these jobs clean, validate, transform, enrich, and prepare the data for analytics in Hive.

---

## Folder Structure

```text
spark/
├── bronze_to_silver.py
├── silver_to_gold.py
└── README.md
```

---

# Spark Pipeline Architecture

The complete Spark transformation flow is:

```text
                         HDFS
                          │
                          ▼
                    BRONZE LAYER
                          │
                          │
                          ▼
               bronze_to_silver.py
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
       SILVER CLEAN DATA         REJECTED DATA
             │                         │
             │                         ▼
             │                  /data/silver/_rejected
             │
             ▼
                 silver_to_gold.py
                          │
                          ▼
                    GOLD LAYER
                          │
             ┌────────────┼────────────┐
             │            │            │
             ▼            ▼            ▼
        dim_customer   fact_customer   monthly_churn
                       _activity          _rate
             │
             ▼
           Hive
             │
             ▼
          Analytics
```

---

## Architecture Image

Place the Spark architecture image here:

```md
![Spark Architecture](../docs/screenshots/spark-architecture.png)
```

---

# 1. Bronze to Silver

## Script

```text
bronze_to_silver.py
```

The `bronze_to_silver.py` job reads raw data from the HDFS Bronze layer and converts it into clean and validated Silver datasets.

The main goals of this job are:

- Clean the raw data
- Validate business rules
- Detect duplicate records
- Validate required fields
- Validate data types and values
- Validate dates and months
- Validate customer relationships
- Separate valid and invalid records
- Keep an audit trail of rejected records

---

## Bronze Input

The job reads the following datasets:

```text
/data/bronze/customers
/data/bronze/customer_support_tickets
/data/bronze/offers
/data/bronze/usage
```

The Bronze layer contains the raw data produced by the ingestion stage.

### Customers

```text
/data/bronze/customers
```

Customer data is loaded through the MariaDB + Sqoop ingestion process.

### Customer Support Tickets

```text
/data/bronze/customer_support_tickets
```

Ticket data is ingested through Apache NiFi.

### Offers

```text
/data/bronze/offers
```

Offer data is ingested through Apache NiFi.

### Usage

```text
/data/bronze/usage
```

Usage data is ingested through Apache NiFi.

---

# Bronze to Silver Processing

The job processes each source independently and applies data-quality rules.

## Customers

Customer records are checked for:

- Required customer identifiers
- Valid customer attributes
- Valid age values
- Valid registration information
- Duplicate records
- Invalid values

Valid records are written to the Silver Customers dataset.

Rejected records are written to the rejected-data area.

---

## Customer Support Tickets

Ticket records are checked for:

- Required customer ID
- Valid ticket information
- Valid ticket dates
- Valid resolution information
- Duplicate records
- Valid customer relationships

Only records related to valid customers are accepted into the Silver dataset.

---

## Offers

Offer records are checked and transformed into a consistent structure.

The processing includes:

- Duplicate detection
- Required-field validation
- Data type validation
- Offer acceptance validation
- Standardization of values

The `accepted` field is represented as an integer value:

```text
0 = Not accepted
1 = Accepted
```

---

## Usage

Usage records are validated for:

- Required customer ID
- Required `usage_log_id`
- Valid usage month
- Valid numeric values
- Valid customer relationships
- Duplicate records

The `usage_log_id` remains a string because the source contains identifiers such as:

```text
USG553504902
```

The usage month is validated as:

```text
YYYY-MM
```

and invalid or future months are rejected.

---

# Duplicate Detection

The Silver job detects exact duplicate records.

The duplicate comparison uses the complete record structure rather than relying on simple string concatenation.

This prevents null values from being incorrectly treated as identical or different during duplicate detection.

---

# Customer Reference Validation

A customer ID reference dataset is created from the final clean Customers data.

The reference data is stored at:

```text
/data/silver/_reference/customer_ids
```

This reference is used to validate customer IDs in related datasets such as:

```text
Customer_support_tickets
Usage
```

This prevents invalid customer records from entering the Silver layer.

---

# Silver Output

Clean records are written to:

```text
/data/silver/customers
/data/silver/customer_support_tickets
/data/silver/offers
/data/silver/usage
```

Rejected records are written to:

```text
/data/silver/_rejected/
```

Customer reference IDs are written to:

```text
/data/silver/_reference/customer_ids
```

---

# Silver Reconciliation

The Bronze-to-Silver job verifies that every source record is accounted for.

For each dataset:

```text
Source Records = Clean Records + Rejected Records
```

The pipeline calculates:

```text
unaccounted = source - clean - rejected
```

The expected result is:

```text
unaccounted = 0
```

A non-zero value indicates that records were lost during processing.

---

# Bronze to Silver Results

The validated run produced:

```text
Customers:
1023 source
684 clean
339 rejected
0 unaccounted

Tickets:
1528 source
901 clean
627 rejected
0 unaccounted

Offers:
1535 source
1417 clean
118 rejected
0 unaccounted

Usage:
3087 source
1935 clean
1152 rejected
0 unaccounted
```

This confirms that all source records were either accepted or rejected.

---

# 2. Silver to Gold

## Script

```text
silver_to_gold.py
```

The `silver_to_gold.py` job reads the validated Silver datasets and creates business-ready Gold datasets for analytics.

The Gold layer is designed around a dimensional model containing:

- Customer dimension
- Customer activity fact
- Monthly churn summary

---

# Silver Input

The job reads:

```text
/data/silver/customers
/data/silver/customer_support_tickets
/data/silver/offers
/data/silver/usage
```

Only validated Silver data is used for the Gold transformation.

---

# Gold Data Model

The Gold layer contains three main datasets:

```text
/data/gold/dim_customer
/data/gold/fact_customer_activity
/data/gold/monthly_churn_rate
```

---

# 2.1 dim_customer

## Output

```text
/data/gold/dim_customer
```

The customer dimension contains one record per customer.

It provides the main customer-level attributes used for analytics.

### Main columns

```text
cust_key
customer_id
is_churned
CLV_LTV
avg_ticket_res_time_hrs
num_products
dw_start_date
dw_end_date
```

### Key Generation

`cust_key` is generated using a Spark `row_number()` window.

The resulting key is a 64-bit integer and is stored as `BIGINT` in Hive.

---

## Customer Churn

The dimension contains:

```text
is_churned
```

This indicates the customer's churn status.

The value is validated so that it represents a valid churn indicator.

---

## Customer Lifetime Value

The dimension contains:

```text
CLV_LTV
```

This represents the customer lifetime value metric used by the pipeline.

The Gold validation checks that the value is not negative.

---

## Ticket Resolution Time

The dimension contains:

```text
avg_ticket_res_time_hrs
```

This represents the average ticket resolution time for the customer.

Negative values are not allowed.

---

## Number of Products

The dimension contains:

```text
num_products
```

This represents the number of products associated with the customer.

---

# 2.2 fact_customer_activity

## Output

```text
/data/gold/fact_customer_activity
```

The customer activity fact stores customer activity at the monthly level.

Each record represents:

```text
one customer + one activity month
```

### Main columns

```text
customer_id
activity_month
usage_events
total_balance
avg_balance
ticket_count
avg_resolution_time
offer_count
accepted_offers
acceptance_rate
cust_key
activity_key
```

---

## Activity Month

The fact table uses:

```text
activity_month
```

to organize activity by month.

The month is represented using:

```text
YYYY-MM
```

Invalid activity months are rejected by validation.

---

## Usage Metrics

The fact table calculates usage information such as:

```text
usage_events
total_balance
avg_balance
```

These metrics summarize customer usage activity for each month.

---

## Ticket Metrics

The fact table calculates:

```text
ticket_count
avg_resolution_time
```

These metrics summarize customer support activity for each month.

---

## Offer Metrics

The fact table calculates:

```text
offer_count
accepted_offers
acceptance_rate
```

The `accepted` source value uses:

```text
1 = accepted
0 = not accepted
```

The Gold transformation therefore counts accepted offers using:

```text
accepted == 1
```

The acceptance rate is calculated from the number of accepted offers and total offers.

---

## Customer Key

The activity fact is linked to the customer dimension through:

```text
cust_key
```

The Gold pipeline uses valid customer records when building the fact table.

This prevents orphan fact records and null customer keys.

---

## Activity Key

Each fact record receives an:

```text
activity_key
```

generated using Spark `row_number()`.

The resulting key is stored as `BIGINT` in Hive.

---

# 2.3 monthly_churn_rate

## Output

```text
/data/gold/monthly_churn_rate
```

This dataset provides monthly churn metrics.

### Main columns

```text
activity_month
active_customers
churned_customers
churn_rate
```

---

## Active Customers

```text
active_customers
```

represents the number of active customers for each activity month.

---

## Churned Customers

```text
churned_customers
```

represents the number of churned customers for each activity month.

---

## Churn Rate

```text
churn_rate
```

represents the monthly churn ratio derived from the active and churned customer counts.

The Gold validation checks that the churn rate remains within a valid range.

---

# Gold Data Relationships

The Gold model can be represented as:

```text
                  dim_customer
                       │
                       │ cust_key
                       │
                       ▼
             fact_customer_activity
                       │
                       │ activity_month
                       ▼
              monthly_churn_rate
```

The customer dimension is the main customer-level table, while the activity fact stores monthly customer behavior.

---

# Gold Data Quality Validation

The `silver_to_gold.py` job validates the resulting Gold data.

Important checks include:

```text
NULL customer_id
NULL cust_key
Duplicate customer_id
Duplicate cust_key
Invalid is_churned
Negative CLV_LTV
Negative average ticket resolution time
NULL activity_key
Duplicate activity_key
Duplicate customer + month
Orphan fact rows
Invalid activity month
Negative usage_events
Negative total_balance
Negative ticket_count
Negative offer_count
Negative accepted_offers
Invalid acceptance_rate
Invalid churn_rate
```

The expected result for all validation errors is:

```text
0
```

A successful pipeline ends with:

```text
GOLD LAYER STATUS: PASS
```

---

# Running the Spark Jobs

Before running the Spark scripts, make sure Hadoop/HDFS and Spark are running.

## Bronze to Silver

```bash
spark-submit ~/Desktop/bronze_to_silver.py
```

## Silver to Gold

```bash
spark-submit ~/Desktop/silver_to_gold.py
```

---

# Recommended Execution Order

The Spark jobs must run in this order:

```text
1. Bronze ingestion
       │
       ▼
2. bronze_to_silver.py
       │
       ▼
3. Silver validation
       │
       ▼
4. silver_to_gold.py
       │
       ▼
5. Gold validation
       │
       ▼
6. Hive / Analytics
```

The Gold job should not be executed before the Silver layer has been successfully created and validated.

---

# HDFS Paths

## Bronze

```text
/data/bronze/customers
/data/bronze/customer_support_tickets
/data/bronze/offers
/data/bronze/usage
```

## Silver

```text
/data/silver/customers
/data/silver/customer_support_tickets
/data/silver/offers
/data/silver/usage
/data/silver/_reference/customer_ids
/data/silver/_rejected/
```

## Gold

```text
/data/gold/dim_customer
/data/gold/fact_customer_activity
/data/gold/monthly_churn_rate
```

---

# Technology Stack

The Spark transformation layer uses:

```text
Apache Spark
PySpark
Python
HDFS
Hive
```

---

# Complete ETL Flow

```text
                         SOURCE
                           │
            ┌──────────────┼──────────────┐
            │              │              │
            ▼              ▼              ▼
       Customers        Tickets         Offers
            │              │              │
            └──────────────┼──────────────┘
                           │
                         Usage
                           │
                           ▼
                    INGESTION LAYER
                 NiFi / MariaDB / Sqoop
                           │
                           ▼
                    HDFS BRONZE
                           │
                           ▼
              bronze_to_silver.py
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
        HDFS SILVER                REJECTED
              │
              ▼
              silver_to_gold.py
              │
       ┌──────┼──────────────┐
       │      │              │
       ▼      ▼              ▼
     DIM     FACT        MONTHLY CHURN
  CUSTOMER  ACTIVITY         RATE
       │      │              │
       └──────┼──────────────┘
              │
              ▼
             HIVE
              │
              ▼
         ANALYTICS
```

---

# Summary

The Spark folder implements the core transformation logic of the Customer Churn ETL Pipeline.

`bronze_to_silver.py` is responsible for:

```text
Raw Bronze
   ↓
Cleaning
   ↓
Validation
   ↓
Deduplication
   ↓
Reference checks
   ↓
Silver + Rejected
```

`silver_to_gold.py` is responsible for:

```text
Validated Silver
   ↓
Business transformations
   ↓
Customer dimension
   ↓
Customer activity fact
   ↓
Monthly churn metrics
   ↓
Gold
```

The final Gold layer is validated before being consumed by Hive and analytics tools.
