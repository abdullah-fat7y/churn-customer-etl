# Source Data

This folder contains the raw source files used by the Customer Churn ETL Pipeline.

The source data is the starting point of the pipeline. The files are loaded into the Bronze layer and then processed through the Silver and Gold layers.

---

## Source Files

| File | Format | Source | Used For |
|---|---|---|---|
| `Customers.csv` | CSV | Customer source data | Customer master data |
| `Customer_support_tickets.csv` | CSV | Customer support system | Support ticket activity |
| `Offers.csv` | CSV | Customer offers system | Offer and acceptance activity |
| `Usage.json` | JSON | Customer usage system | Customer usage activity |

---

## 1. Customers.csv

`Customers.csv` contains the main customer information used by the pipeline.

Typical business fields include:

- Customer ID
- Customer name
- Age
- Registration date
- Email
- Customer-related attributes used for churn analysis

### Ingestion

The customer data follows this path:

```text
Customers.csv
    ↓
MariaDB
    ↓
Sqoop
    ↓
HDFS Bronze
    ↓
/data/bronze/customers
```

The customer source file is located at:

```text
/home/hadoop/Desktop/Customers.csv
```

---

## 2. Customer_support_tickets.csv

`Customer_support_tickets.csv` contains customer support interactions and ticket information.

The data is used to analyze:

- Number of support tickets
- Ticket resolution time
- Customer support activity
- Customer activity by month

### Ingestion

The file is processed by Apache NiFi:

```text
Customer_support_tickets.csv
    ↓
Apache NiFi
    ↓
HDFS Bronze
    ↓
/data/bronze/customer_support_tickets
```

---

## 3. Offers.csv

`Offers.csv` contains customer offer information.

The data is used to analyze:

- Number of offers
- Accepted offers
- Offer acceptance rate
- Customer engagement with offers

### Ingestion

The file is processed by Apache NiFi:

```text
Offers.csv
    ↓
Apache NiFi
    ↓
HDFS Bronze
    ↓
/data/bronze/offers
```

---

## 4. Usage.json

`Usage.json` contains customer usage activity.

The data is used to calculate:

- Usage events
- Customer activity by month
- Customer balances
- Monthly customer activity metrics

### Ingestion

The file is processed by Apache NiFi:

```text
Usage.json
    ↓
Apache NiFi
    ↓
HDFS Bronze
    ↓
/data/bronze/usage
```

---

# Source Data Flow

The complete source ingestion flow is:

```text
                         SOURCE DATA
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          ▼                  ▼                  ▼
   Customers.csv     Customer_support_tickets.csv
          │                  │
          │               NiFi
       MariaDB                │
          │                  │
        Sqoop                 │
          │                  │
          └────────────┬─────┘
                       │
                       ▼
                  HDFS BRONZE
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
     Customers      Tickets       Offers
                                    │
                                    ▼
                                  Usage
```

> **Architecture Image Placeholder**
>
> Add the source-data architecture image here.
>
> `![Source Data Architecture](../docs/screenshots/source-data-architecture.png)`

---

# Data Quality

The source data may contain invalid, missing, duplicate, or inconsistent records.

The pipeline handles these issues during the Silver transformation stage.

Examples of data quality checks include:

- Null or invalid customer IDs
- Invalid dates
- Invalid age values
- Invalid usage months
- Duplicate records
- Missing required fields
- Invalid foreign-key relationships

Records that fail validation are moved to the Silver rejected-data areas.

---

# Source to Bronze Mapping

| Source File | Ingestion Tool | Bronze Location |
|---|---|---|
| `Customers.csv` | MariaDB + Sqoop | `/data/bronze/customers` |
| `Customer_support_tickets.csv` | Apache NiFi | `/data/bronze/customer_support_tickets` |
| `Offers.csv` | Apache NiFi | `/data/bronze/offers` |
| `Usage.json` | Apache NiFi | `/data/bronze/usage` |

---

# Expected Directory Structure

```text
data/
└── source/
    ├── Customers.csv
    ├── Customer_support_tickets.csv
    ├── Offers.csv
    └── Usage.json
```

---

# Pipeline Position

The source data is the first stage of the ETL pipeline:

```text
Source Data
    ↓
Bronze Layer
    ↓
Silver Layer
    ↓
Gold Layer
    ↓
Hive / Analytics
```

The source files should remain unchanged. Data cleaning, validation, transformation, and business logic are performed in later pipeline stages.
