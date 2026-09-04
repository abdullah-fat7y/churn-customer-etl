# Churn Customer ETL Pipeline

A Hadoop/Spark/NiFi/Hive ETL project that moves customer data from source files through Bronze, Silver, and Gold layers for churn analysis and BI.

## Project Pipeline

![Project Pipeline](Documentation/5a90da54-8ee4-4647-8ba9-1414e30c3bda.jfif)

## 1. Project Architecture

```text
Source Files
    |
    +--------------------+
    |                    |
    v                    v
Customers.csv         NiFi Sources
    |                 Tickets / Offers / Usage
    v                    |
MariaDB                  |
    |                    |
    v                    v
Sqoop  ----------------> HDFS Bronze
                             |
                             v
                    PySpark Bronze -> Silver
                             |
                             v
                        HDFS Silver
                             |
                             v
                    PySpark Silver -> Gold
                             |
                             v
                         HDFS Gold
                             |
                             v
                    Hive External Tables
                             |
                             v
                    Hue / BI / Power BI
```

## 2. Repository Structure

```text
churn-customer-etl/
├── README.md
├── .gitignore
│
├── data/
│   ├── source/
│   │   ├── Customers.csv
│   │   ├── Customer_support_tickets.csv
│   │   ├── Offers.csv
│   │   └── Usage.json
│   │
│   ├── bronze/
│   │   ├── README.md
│   │   └── .gitkeep
│   │
│   ├── silver/
│   │   └── .gitkeep
│   │
│   └── gold/
│       └── .gitkeep
│
├── nifi/
│   ├── README.md
│   └── <exported-NiFi-flow>.xml
│
├── spark/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── hive/
│   └── gold_hive_queries_test.hql
│
├── scripts/
│   ├── ingestion.sh
│   ├── run_bronze.sh
│   ├── run_silver.sh
│   ├── run_gold.sh
│   └── run_hive_gold.sh
│
└── docs/
    ├── pipeline_overview.md
    ├── troubleshooting.md
    └── screenshots/
```

## 3. Important Rule About HDFS Data

The folders under `data/bronze`, `data/silver`, and `data/gold` are repository placeholders.

The real pipeline data is stored in HDFS:

```text
/data/bronze
/data/silver
/data/gold
```

Do not commit generated Parquet files, Spark staging files, or HDFS runtime data to Git unless there is a specific reason.

The repository should contain the source files, code, SQL, NiFi flow export, and documentation.

## 4. Environment

Expected environment used by this project:

```text
OS:             CentOS 7
Linux user:     hadoop

Hadoop:
    /home/hadoop/hadoop

Spark:
    /usr/local/spark3/spark-3.1.2-bin-hadoop3.2

NiFi:
    /usr/local/nifi/nifi-1.14.0

Database:
    MariaDB / MySQL
    Database: churn_db
    Customer table: customers

Hive database:
    churn_gold
```

Change the paths in the scripts if your installation is different.

## 5. Source Data

Put these files in:

```text
data/source/
```

Required files:

```text
Customers.csv
Customer_support_tickets.csv
Offers.csv
Usage.json
```

The Customers source is loaded into MariaDB and then imported to HDFS with Sqoop.

The Tickets, Offers, and Usage sources are ingested by NiFi into HDFS Bronze.

## 6. HDFS Layout

### Bronze

```text
/data/bronze/
├── customers/
├── customer_support_tickets/
├── offers/
└── usage/
```

### Silver

```text
/data/silver/
├── customers/
├── customer_support_tickets/
├── offers/
├── usage/
├── _reference/
└── _rejected/
```

### Gold

```text
/data/gold/
├── dim_customer/
├── fact_customer_activity/
└── monthly_churn_rate/
```

## 7. Pipeline Execution Order

Run the project in this order:

```text
1. Prepare source files
2. Start Hadoop / HDFS
3. Start MariaDB
4. Run Customers Bronze ingestion
5. Run NiFi ingestion for Tickets / Offers / Usage
6. Verify Bronze
7. Run Bronze -> Silver
8. Verify Silver
9. Run Silver -> Gold
10. Verify Gold
11. Create Hive external tables
12. Run Hive tests and business queries
13. Connect BI tools
```

## 8. Step 0 - Start Services

Start Hadoop services according to your local Hadoop installation.

Check:

```bash
jps
```

You should see the required Hadoop services for your setup.

Check HDFS:

```bash
hdfs dfs -ls /
```

Check MariaDB:

```bash
sudo systemctl status mariadb
```

Start it when required:

```bash
sudo systemctl start mariadb
```

## 9. Step 1 - Clean HDFS and Start From Scratch

For a complete reset of this project:

```bash
hdfs dfs -rm -r -f /data/bronze
hdfs dfs -rm -r -f /data/silver
hdfs dfs -rm -r -f /data/rejected
hdfs dfs -rm -r -f /data/gold
hdfs dfs -rm -r -f /data/_staging
```

Then:

```bash
hdfs dfs -mkdir -p /data
```

Verify:

```bash
hdfs dfs -ls /data
```

Do not run the reset commands on a shared or production HDFS cluster.

## 10. Step 2 - Customers Bronze Ingestion

The Customers flow is:

```text
Customers.csv
    |
    v
MariaDB churn_db.customers
    |
    v
Sqoop
    |
    v
/data/bronze/customers
```

Run:

```bash
chmod +x scripts/ingestion.sh
export SAFE_MODE=0
./scripts/ingestion.sh
```

Verify:

```bash
hdfs dfs -ls /data/bronze/customers
```

Count records:

```bash
hdfs dfs -cat /data/bronze/customers/part-* | wc -l
```

Check MariaDB:

```bash
mysql -u root -phadoop -e \
"SELECT COUNT(*) AS customer_count FROM churn_db.customers;"
```

## 11. Step 3 - NiFi Bronze Ingestion

NiFi handles:

```text
Customer_support_tickets.csv -> /data/bronze/customer_support_tickets
Offers.csv                    -> /data/bronze/offers
Usage.json                    -> /data/bronze/usage
```

Before starting NiFi processors, make sure the HDFS permissions allow the NiFi runtime user to write to the Bronze directories.

Verify:

```bash
hdfs dfs -ls -R /data/bronze
```

The NiFi flow should complete without FlowFiles going to the failure relationship.

After NiFi finishes:

```bash
hdfs dfs -ls -R /data/bronze
```

## 12. Step 4 - Bronze Verification

Expected Bronze structure:

```text
/data/bronze/customers
/data/bronze/customer_support_tickets
/data/bronze/offers
/data/bronze/usage
```

Check:

```bash
hdfs dfs -ls -R /data/bronze
```

Do not assume Usage line count equals record count because `Usage.json` is a JSON array.

## 13. Step 5 - Bronze -> Silver

Script:

```text
spark/bronze_to_silver.py
```

Run:

```bash
python3 -m py_compile spark/bronze_to_silver.py
```

Then:

```bash
/usr/local/spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
spark/bronze_to_silver.py 2>&1 | tee silver_run.log
```

The pipeline processes:

```text
Customers
Tickets
Offers
Usage
```

For each source it performs:

```text
Stage 1 - Standardize + Cast
Stage 2 - Exact duplicate removal
Stage 3 - Validation
Stage 4 - Business-key deduplication + Silver write
```

The pipeline also creates the final valid CustomerId reference used by Tickets and Usage foreign-key validation.

## 14. Step 6 - Verify Silver

Run:

```bash
grep -Ei \
"FINAL REPORT|Source rows|Clean rows|Rejected rows|Unaccounted rows|FAILED|ERROR|Exception" \
silver_run.log
```

A healthy run should have:

```text
Unaccounted rows : 0
```

for every source.

Verify HDFS:

```bash
hdfs dfs -ls -R /data/silver
```

Expected:

```text
/data/silver/customers
/data/silver/customer_support_tickets
/data/silver/offers
/data/silver/usage
/data/silver/_reference/customer_ids
/data/silver/_rejected/
```

## 15. Step 7 - Silver -> Gold

Script:

```text
spark/silver_to_gold.py
```

Syntax check:

```bash
python3 -m py_compile spark/silver_to_gold.py
```

For a fresh Gold build:

```bash
export SAFE_MODE=0
export DEBUG=0
```

Run:

```bash
/usr/local/spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
spark/silver_to_gold.py 2>&1 | tee gold_run.log
```

Gold datasets:

```text
dim_customer
fact_customer_activity
monthly_churn_rate
```

## 16. Step 8 - Verify Gold

Run:

```bash
grep -Ei \
"GOLD LAYER STATUS|ROW COUNTS|NULL|Duplicate|Orphan|Invalid|Negative|ERROR|Exception" \
gold_run.log
```

The final line must be:

```text
GOLD LAYER STATUS: PASS
```

Important checks include:

```text
NULL cust_key = 0
Duplicate cust_key = 0
Duplicate customer_id = 0
NULL activity_key = 0
Duplicate activity_key = 0
Duplicate customer + month = 0
Orphan fact rows = 0
Invalid acceptance_rate = 0
Invalid churn_rate = 0
```

Verify HDFS:

```bash
hdfs dfs -ls -R /data/gold
```

## 17. Step 9 - Hive External Tables

The Hive file is:

```text
hive/gold_hive_queries_test.hql
```

It creates:

```text
churn_gold.dim_customer_gold
churn_gold.fact_customer_activity_gold
churn_gold.monthly_churn_rate_gold
```

Run from the terminal:

```bash
hive -f hive/gold_hive_queries_test.hql
```

Or paste the HQL into the Hue Hive editor.

The tables point to:

```text
/data/gold/dim_customer
/data/gold/fact_customer_activity
/data/gold/monthly_churn_rate
```

## 18. Step 10 - Hive Verification

Use:

```sql
USE churn_gold;

SHOW TABLES;

SELECT COUNT(*) FROM dim_customer_gold;

SELECT COUNT(*) FROM fact_customer_activity_gold;

SELECT COUNT(*) FROM monthly_churn_rate_gold;
```

Key integrity query:

```sql
SELECT COUNT(*) AS orphan_fact_rows
FROM fact_customer_activity_gold f
LEFT JOIN dim_customer_gold d
    ON f.cust_key = d.cust_key
WHERE d.cust_key IS NULL;
```

Expected:

```text
0
```

## 19. Business Questions

The Hive script contains analysis queries for:

```text
1. Highest-value customers
2. High-value customers and churn risk
3. Months with highest churn
4. Churn trend
5. Customers with most support tickets
6. Support resolution time vs churn
7. Offer effectiveness
8. Number of products vs customer value
9. Balance vs churn
10. Engagement vs churn
11. Number of products vs churn
12. Retention-priority customers
13. Customer churn-risk segmentation
```

## 20. Useful Rerun Commands

### Re-run Silver

```bash
/usr/local/spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
spark/bronze_to_silver.py 2>&1 | tee silver_run.log
```

### Re-run Gold

```bash
export SAFE_MODE=0
export DEBUG=0

/usr/local/spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
spark/silver_to_gold.py 2>&1 | tee gold_run.log
```

### Re-create Hive tables

```bash
hive -f hive/gold_hive_queries_test.hql
```

## 21. Troubleshooting

### HDFS Permission Denied

Example:

```text
Permission denied: user=root, access=WRITE
```

Check:

```bash
hdfs dfs -ls -ld /data/bronze
```

Check the actual NiFi runtime user and make sure that user has HDFS write permission.

### Bronze Customers Missing

Check:

```bash
ls -lh data/source/Customers.csv
```

and:

```bash
hdfs dfs -ls /data/bronze/customers
```

### Usage Records All Rejected

Check the schema and make sure:

```text
usage_log_id
```

is treated as a STRING.

Values look like:

```text
USG553504902
```

They must not be cast to BIGINT.

### Silver Pipeline Fails

Check:

```bash
grep -Ei "ERROR|Exception|FAILED" silver_run.log
```

Then inspect the final report for the source that failed.

### Gold Pipeline Fails

Check:

```bash
grep -Ei "ERROR|Exception|FAILED" gold_run.log
```

Then inspect:

```text
GOLD DATA QUALITY REPORT
```

### Hive Table Warning

Check the actual Hive table schema:

```sql
DESCRIBE dim_customer_gold;
DESCRIBE fact_customer_activity_gold;
DESCRIBE monthly_churn_rate_gold;
```

The Gold surrogate keys should be BIGINT.

## 22. Final Data Flow

```text
Customers.csv
     |
     v
MariaDB
     |
     v
Sqoop
     |
     +----------------------+
                            |
Tickets.csv  --------------+
Offers.csv   --------------+----> HDFS Bronze
Usage.json   --------------+
                            |
                            v
                    PySpark Bronze -> Silver
                            |
                            v
                       HDFS Silver
                            |
                            v
                    PySpark Silver -> Gold
                            |
                            v
                        HDFS Gold
                            |
                            v
                    Hive External Tables
                            |
                            v
                       Hue / BI
```

## 26. Current Verified State

The project has been tested through the Gold layer.

The latest successful Gold validation produced:

```text
GOLD LAYER STATUS: PASS
```

The Gold quality checks showed zero:

```text
NULL customer keys
Duplicate customer keys
Duplicate customer IDs
NULL activity keys
Duplicate activity keys
Duplicate customer + month
Orphan fact rows
Invalid activity months
Negative measures
Invalid acceptance rates
Invalid churn rates
```
