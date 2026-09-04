#!/usr/bin/env python3

# ============================================================
# SILVER -> GOLD TRANSFORMATION PIPELINE
# ============================================================
#
# Platform:
#     CentOS 7
#     Hadoop HDFS
#     Spark 3.1.2
#     PySpark
#
# Purpose:
#     Transform the cleaned Silver datasets into trusted Gold
#     datasets for reporting, BI, and analytics.
#
# Silver inputs:
#     /data/silver/customers
#     /data/silver/customer_support_tickets
#     /data/silver/offers
#     /data/silver/usage
#
# Gold outputs:
#     /data/gold/dim_customer
#     /data/gold/fact_customer_activity
#     /data/gold/monthly_churn_rate
#
# Gold data model:
#
#     1. dim_customer
#        One row per valid customer.
#
#        Contains:
#          - cust_key
#          - customer_id
#          - is_churned
#          - CLV_LTV
#          - avg_ticket_res_time_hrs
#          - num_products
#          - dw_start_date
#          - dw_end_date
#
#     2. fact_customer_activity
#        One row per valid customer + activity month.
#
#        Combines:
#          - usage activity
#          - support ticket activity
#          - offer activity
#
#        Contains:
#          - customer_id
#          - activity_month
#          - usage_events
#          - total_balance
#          - avg_balance
#          - ticket_count
#          - avg_resolution_time
#          - offer_count
#          - accepted_offers
#          - acceptance_rate
#          - cust_key
#          - activity_key
#
#     3. monthly_churn_rate
#        One row per activity month.
#
#        Contains:
#          - active_customers
#          - churned_customers
#          - churn_rate
#
# Processing flow:
#
#     Silver Customers
#            |
#            v
#     dim_customer
#            |
#            +------------------+
#            |                  |
#            v                  v
#     Customer ID FK      Monthly Activity
#       reference               |
#                              v
#                     fact_customer_activity
#                              |
#                              v
#                     monthly_churn_rate
#
# Important data-quality rules:
#     - Only final valid Customers can appear in Gold.
#     - Tickets and Usage must reference valid Customers.
#     - Offers are validated in Silver but are filtered against
#       the final Gold customer dimension before entering the fact.
#     - One customer = one dim_customer row.
#     - One customer + month = one fact row.
#     - cust_key must be unique and not NULL.
#     - activity_key must be unique and not NULL.
#     - No orphan fact rows are allowed.
#     - acceptance_rate must be between 0 and 1.
#     - churn_rate must be between 0 and 1.
#     - accepted_offers cannot be greater than offer_count.
#     - Negative activity measures are not allowed.
#
# Safety:
#     SAFE_MODE=1 by default.
#     Existing Gold directories cause the script to stop.
#
#     Use SAFE_MODE=0 to replace existing Gold output.
#
# Debug:
#     DEBUG=1 prints schemas, samples, counts, and validation
#     checks.
#
# Reconciliation:
#     The script performs multiple integrity checks before
#     declaring the Gold layer as PASS.
#
# ============================================================


import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

SAFE_MODE = os.getenv("SAFE_MODE", "1")
DEBUG = os.getenv("DEBUG", "1")

SILVER_ROOT = "hdfs:///data/silver"
GOLD_ROOT = "hdfs:///data/gold"

CUSTOMERS_PATH = f"{SILVER_ROOT}/customers"
TICKETS_PATH = f"{SILVER_ROOT}/customer_support_tickets"
OFFERS_PATH = f"{SILVER_ROOT}/offers"
USAGE_PATH = f"{SILVER_ROOT}/usage"

DIM_CUSTOMER_PATH = f"{GOLD_ROOT}/dim_customer"
FACT_ACTIVITY_PATH = f"{GOLD_ROOT}/fact_customer_activity"
MONTHLY_CHURN_PATH = f"{GOLD_ROOT}/monthly_churn_rate"


# Warehouse validity dates.
#
# Override with:
#     export DW_START_DATE=YYYY-MM-DD
#
# The default is the date used in the supplied Gold design.
DW_START_DATE = os.getenv(
    "DW_START_DATE",
    "2026-09-03"
)

DW_END_DATE = "9999-12-31"


# ============================================================
# LOGGING
# ============================================================

def log(message):
    print()
    print("=" * 70)
    print(message)
    print("=" * 70)


def debug(message):
    if DEBUG == "1":
        print("[DEBUG]", message)


def die(message):
    print()
    print("=" * 70)
    print("ERROR:", message)
    print("=" * 70)
    sys.exit(1)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Churn_Silver_To_Gold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HDFS HELPERS
# ============================================================

def get_fs():
    return (
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
    )


def hdfs_exists(path):
    fs = get_fs()
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return fs.exists(p)


def delete_hdfs(path):
    fs = get_fs()
    p = spark._jvm.org.apache.hadoop.fs.Path(path)

    if fs.exists(p):
        fs.delete(p, True)


def prepare_output(path):
    if not hdfs_exists(path):
        return

    if SAFE_MODE == "1":
        die(
            f"Gold output already exists: {path}\n"
            f"SAFE_MODE=1 prevents overwrite.\n"
            f"Use SAFE_MODE=0 to replace it."
        )

    print(
        "SAFE_MODE=0: removing existing output:",
        path
    )

    delete_hdfs(path)


def require_path(path):
    if not hdfs_exists(path):
        die(
            f"Required Silver path does not exist: {path}"
        )


# ============================================================
# DEBUG HELPERS
# ============================================================

def show_debug(df, name):

    if DEBUG != "1":
        return

    print()
    print("-----", name, "SCHEMA -----")
    df.printSchema()

    print()
    print("-----", name, "SAMPLE -----")
    df.show(
        10,
        truncate=False
    )

    print()
    print("-----", name, "COUNT -----")
    print(df.count())


# ============================================================
# STEP 0 - ENVIRONMENT CHECK
# ============================================================

log("STEP 0 - Checking environment")

print("SAFE_MODE:", SAFE_MODE)
print("DEBUG:", DEBUG)
print("Spark version:", spark.version)
print("Silver root:", SILVER_ROOT)
print("Gold root:", GOLD_ROOT)
print("DW_START_DATE:", DW_START_DATE)
print("DW_END_DATE:", DW_END_DATE)

for path in [
    CUSTOMERS_PATH,
    TICKETS_PATH,
    OFFERS_PATH,
    USAGE_PATH,
]:
    require_path(path)

print("All Silver inputs exist.")


# ============================================================
# STEP 1 - READ SILVER
# ============================================================

log("STEP 1 - Reading Silver")

customers = spark.read.parquet(
    CUSTOMERS_PATH
)

tickets = spark.read.parquet(
    TICKETS_PATH
)

offers = spark.read.parquet(
    OFFERS_PATH
)

usage = spark.read.parquet(
    USAGE_PATH
)

show_debug(
    customers,
    "SILVER CUSTOMERS"
)

show_debug(
    tickets,
    "SILVER TICKETS"
)

show_debug(
    offers,
    "SILVER OFFERS"
)

show_debug(
    usage,
    "SILVER USAGE"
)

print()
print("Silver row counts:")
print("Customers:", customers.count())
print("Tickets:", tickets.count())
print("Offers:", offers.count())
print("Usage:", usage.count())


# ============================================================
# STEP 2 - STANDARDIZE CUSTOMER KEYS
# ============================================================

log("STEP 2 - Standardizing Customer Keys")


# Customers Silver normally contains CustomerId.
# Support alternative casing for compatibility.

if "CustomerId" in customers.columns:

    customers = customers.withColumn(
        "customer_id",
        F.col("CustomerId").cast("long")
    )

elif "customerid" in customers.columns:

    customers = customers.withColumn(
        "customer_id",
        F.col("customerid").cast("long")
    )

elif "customer_id" not in customers.columns:

    die(
        "Customers Silver does not contain a customer ID column."
    )


tickets = tickets.withColumn(
    "customer_id",
    F.col("customer_id").cast("long")
)

offers = offers.withColumn(
    "customer_id",
    F.col("customer_id").cast("long")
)

usage = usage.withColumn(
    "customer_id",
    F.col("customer_id").cast("long")
)

debug(
    "Customer IDs standardized."
)


# ============================================================
# STEP 3 - PREPARE ACTIVITY MONTHS
# ============================================================

log("STEP 3 - Preparing Activity Months")


# ------------------------------------------------------------
# Usage activity
# ------------------------------------------------------------

if "usage_month_date" in usage.columns:

    usage_activity = usage.withColumn(
        "activity_month",
        F.date_format(
            F.col("usage_month_date"),
            "yyyy-MM"
        )
    )

else:

    usage_activity = usage.withColumn(
        "activity_month",
        F.when(
            F.col("usage_month").rlike(
                r"^\d{4}-(0[1-9]|1[0-2])$"
            ),
            F.col("usage_month")
        )
        .when(
            F.col("usage_month").rlike(
                r"^\d{4}/(0[1-9]|1[0-2])$"
            ),
            F.regexp_replace(
                F.col("usage_month"),
                "/",
                "-"
            )
        )
        .when(
            F.col("usage_month").rlike(
                r"^(0[1-9]|1[0-2])/\d{4}$"
            ),
            F.concat(
                F.regexp_extract(
                    F.col("usage_month"),
                    r"^(0[1-9]|1[0-2])/(\d{4})$",
                    2
                ),
                F.lit("-"),
                F.regexp_extract(
                    F.col("usage_month"),
                    r"^(\d{2})/\d{4}$",
                    1
                )
            )
        )
        .otherwise(None)
    )


# ------------------------------------------------------------
# Ticket activity
# ------------------------------------------------------------

tickets_activity = tickets.withColumn(
    "activity_month",
    F.date_format(
        F.col("created_at"),
        "yyyy-MM"
    )
)


# ------------------------------------------------------------
# Offer activity
# ------------------------------------------------------------

offers_activity = offers.withColumn(
    "activity_month",
    F.date_format(
        F.col("date_offered"),
        "yyyy-MM"
    )
)


# ============================================================
# STEP 4 - CUSTOMER AGGREGATIONS
# ============================================================

log("STEP 4 - Building Customer Aggregations")


# ------------------------------------------------------------
# CLV / LTV
# ------------------------------------------------------------

clv_agg = (
    usage
    .groupBy("customer_id")
    .agg(
        F.sum(
            "monthly_balance"
        ).alias("CLV_LTV")
    )
)

show_debug(
    clv_agg,
    "CLV / LTV"
)


# ------------------------------------------------------------
# Average ticket resolution time
# ------------------------------------------------------------

ticket_agg = (
    tickets
    .groupBy("customer_id")
    .agg(
        F.avg(
            "resolution_time_hrs"
        ).alias(
            "avg_ticket_res_time_hrs"
        )
    )
)

show_debug(
    ticket_agg,
    "TICKET AGGREGATION"
)


# ============================================================
# STEP 5 - BUILD DIM_CUSTOMER
# ============================================================

log("STEP 5 - Building dim_customer")


# Normalize column names explicitly.
#
# The Silver customer dataset contains:
#     Exited
#     NumOfProducts
#
# We rename them to the Gold naming convention.

if "Exited" in customers.columns:

    exited_col = F.col("Exited")

elif "exited" in customers.columns:

    exited_col = F.col("exited")

else:

    die(
        "Customers Silver does not contain Exited."
    )


if "NumOfProducts" in customers.columns:

    num_products_col = F.col(
        "NumOfProducts"
    )

elif "numofproducts" in customers.columns:

    num_products_col = F.col(
        "numofproducts"
    )

else:

    die(
        "Customers Silver does not contain NumOfProducts."
    )


dim_customer_base = (
    customers
    .select(
        "customer_id",
        exited_col
            .cast("int")
            .alias("is_churned"),

        num_products_col
            .cast("int")
            .alias("num_products")
    )
    .filter(
        F.col("customer_id").isNotNull()
    )
    .dropDuplicates(
        ["customer_id"]
    )
)


# Add customer-level metrics.

dim_customer = (
    dim_customer_base

    .join(
        clv_agg,
        on="customer_id",
        how="left"
    )

    .join(
        ticket_agg,
        on="customer_id",
        how="left"
    )

    .withColumn(
        "CLV_LTV",
        F.coalesce(
            F.col("CLV_LTV"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "avg_ticket_res_time_hrs",
        F.coalesce(
            F.col(
                "avg_ticket_res_time_hrs"
            ),
            F.lit(0.0)
        )
    )

    .withColumn(
        "dw_start_date",
        F.to_date(
            F.lit(DW_START_DATE)
        )
    )

    .withColumn(
        "dw_end_date",
        F.to_date(
            F.lit(DW_END_DATE)
        )
    )
)


# ------------------------------------------------------------
# Generate customer surrogate key
# ------------------------------------------------------------

customer_window = Window.orderBy(
    "customer_id"
)


dim_customer = (
    dim_customer
    .withColumn(
        "cust_key",
        F.row_number().over(
            customer_window
        )
    )

    .select(
        "cust_key",
        "customer_id",
        "is_churned",
        "CLV_LTV",
        "avg_ticket_res_time_hrs",
        "num_products",
        "dw_start_date",
        "dw_end_date"
    )
)


show_debug(
    dim_customer,
    "DIM_CUSTOMER"
)


# ============================================================
# STEP 6 - PREPARE FINAL CUSTOMER FK REFERENCE
# ============================================================

log("STEP 6 - Preparing Final Customer Reference")


# Only customers that actually exist in the final dimension
# are allowed into the Gold fact table.

valid_customer_ids = (
    dim_customer
    .select("customer_id")
    .filter(
        F.col("customer_id").isNotNull()
    )
    .distinct()
)


print(
    "Final valid customers:",
    valid_customer_ids.count()
)


# ============================================================
# STEP 7 - MONTHLY USAGE AGGREGATION
# ============================================================

log("STEP 7 - Building Monthly Usage Aggregation")


usage_monthly = (
    usage_activity

    .filter(
        F.col("activity_month").isNotNull()
    )

    .groupBy(
        "customer_id",
        "activity_month"
    )

    .agg(
        F.count(
            "usage_log_id"
        ).alias(
            "usage_events"
        ),

        F.sum(
            "monthly_balance"
        ).alias(
            "total_balance"
        ),

        F.avg(
            "monthly_balance"
        ).alias(
            "avg_balance"
        )
    )
)


show_debug(
    usage_monthly,
    "USAGE MONTHLY"
)


# ============================================================
# STEP 8 - MONTHLY TICKET AGGREGATION
# ============================================================

log("STEP 8 - Building Monthly Ticket Aggregation")


tickets_monthly = (
    tickets_activity

    .filter(
        F.col("activity_month").isNotNull()
    )

    .groupBy(
        "customer_id",
        "activity_month"
    )

    .agg(
        F.count(
            "ticket_id"
        ).alias(
            "ticket_count"
        ),

        F.avg(
            "resolution_time_hrs"
        ).alias(
            "avg_resolution_time"
        )
    )
)


show_debug(
    tickets_monthly,
    "TICKETS MONTHLY"
)


# ============================================================
# STEP 9 - MONTHLY OFFER AGGREGATION
# ============================================================

log("STEP 9 - Building Monthly Offer Aggregation")


offers_monthly = (
    offers_activity

    .filter(
        F.col("activity_month").isNotNull()
    )

    .groupBy(
        "customer_id",
        "activity_month"
    )

    .agg(
        F.count(
            "offer_id"
        ).alias(
            "offer_count"
        ),

        F.sum(
            F.when(
                F.col("accepted").cast("int") == 1,
                F.lit(1)
            )
            .otherwise(
                F.lit(0)
            )
        ).alias(
            "accepted_offers"
        ),

        F.avg(
            F.col("accepted").cast("double")
        ).alias(
            "acceptance_rate"
        )
    )
)


show_debug(
    offers_monthly,
    "OFFERS MONTHLY"
)


# ============================================================
# STEP 10 - BUILD ACTIVITY MONTHS
# ============================================================

log("STEP 10 - Building Activity Months")


activity_months = (
    usage_monthly
    .select(
        "customer_id",
        "activity_month"
    )

    .union(
        tickets_monthly.select(
            "customer_id",
            "activity_month"
        )
    )

    .union(
        offers_monthly.select(
            "customer_id",
            "activity_month"
        )
    )

    .distinct()

    .filter(
        F.col("customer_id").isNotNull()
    )

    .filter(
        F.col("activity_month").isNotNull()
    )

    # Critical FK protection:
    # remove activity belonging to customers that were
    # rejected from the final Customers Silver dataset.
    .join(
        valid_customer_ids,
        on="customer_id",
        how="inner"
    )
)


print(
    "Valid activity rows:",
    activity_months.count()
)

if DEBUG == "1":

    activity_months.show(
        20,
        truncate=False
    )


# ============================================================
# STEP 11 - BUILD FACT_CUSTOMER_ACTIVITY
# ============================================================

log("STEP 11 - Building fact_customer_activity")


fact_customer_activity = (

    activity_months

    # --------------------------------------------------------
    # Usage
    # --------------------------------------------------------

    .join(
        usage_monthly,
        [
            "customer_id",
            "activity_month"
        ],
        "left"
    )

    # --------------------------------------------------------
    # Tickets
    # --------------------------------------------------------

    .join(
        tickets_monthly,
        [
            "customer_id",
            "activity_month"
        ],
        "left"
    )

    # --------------------------------------------------------
    # Offers
    # --------------------------------------------------------

    .join(
        offers_monthly,
        [
            "customer_id",
            "activity_month"
        ],
        "left"
    )

    # --------------------------------------------------------
    # Missing activity means zero.
    # --------------------------------------------------------

    .withColumn(
        "usage_events",
        F.coalesce(
            F.col("usage_events"),
            F.lit(0)
        )
    )

    .withColumn(
        "total_balance",
        F.coalesce(
            F.col("total_balance"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "avg_balance",
        F.coalesce(
            F.col("avg_balance"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "ticket_count",
        F.coalesce(
            F.col("ticket_count"),
            F.lit(0)
        )
    )

    .withColumn(
        "avg_resolution_time",
        F.coalesce(
            F.col("avg_resolution_time"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "offer_count",
        F.coalesce(
            F.col("offer_count"),
            F.lit(0)
        )
    )

    .withColumn(
        "accepted_offers",
        F.coalesce(
            F.col("accepted_offers"),
            F.lit(0)
        )
    )

    .withColumn(
        "acceptance_rate",
        F.coalesce(
            F.col("acceptance_rate"),
            F.lit(0.0)
        )
    )

    # --------------------------------------------------------
    # Add customer surrogate key.
    # --------------------------------------------------------

    .join(
        dim_customer.select(
            "customer_id",
            "cust_key"
        ),
        on="customer_id",
        how="inner"
    )
)


# ------------------------------------------------------------
# Generate activity surrogate key
# ------------------------------------------------------------

activity_window = Window.orderBy(
    "customer_id",
    "activity_month"
)


fact_customer_activity = (
    fact_customer_activity

    .withColumn(
        "activity_key",
        F.row_number().over(
            activity_window
        )
    )

    .select(
        "customer_id",
        "activity_month",
        "usage_events",
        "total_balance",
        "avg_balance",
        "ticket_count",
        "avg_resolution_time",
        "offer_count",
        "accepted_offers",
        "acceptance_rate",
        "cust_key",
        "activity_key"
    )
)


show_debug(
    fact_customer_activity,
    "FACT_CUSTOMER_ACTIVITY"
)


# ============================================================
# STEP 12 - BUILD MONTHLY CHURN RATE
# ============================================================

log("STEP 12 - Building Monthly Churn Rate")


monthly_churn = (
    fact_customer_activity

    .join(
        dim_customer.select(
            "customer_id",
            "is_churned"
        ),
        on="customer_id",
        how="inner"
    )

    .groupBy(
        "activity_month"
    )

    .agg(

        F.countDistinct(
            "customer_id"
        ).alias(
            "active_customers"
        ),

        F.countDistinct(
            F.when(
                F.col("is_churned") == 1,
                F.col("customer_id")
            )
        ).alias(
            "churned_customers"
        )
    )

    .withColumn(
        "churn_rate",

        F.when(
            F.col(
                "active_customers"
            ) > 0,

            F.col(
                "churned_customers"
            )
            /
            F.col(
                "active_customers"
            )

        ).otherwise(
            F.lit(0.0)
        )
    )

    .orderBy(
        "activity_month"
    )
)


show_debug(
    monthly_churn,
    "MONTHLY CHURN"
)


# ============================================================
# STEP 13 - PREPARE GOLD OUTPUTS
# ============================================================

log("STEP 13 - Preparing Gold Outputs")


for path in [
    DIM_CUSTOMER_PATH,
    FACT_ACTIVITY_PATH,
    MONTHLY_CHURN_PATH
]:

    prepare_output(path)


# ============================================================
# STEP 14 - WRITE GOLD PARQUET
# ============================================================

log("STEP 14 - Writing Gold Parquet")


# Spark 3.1.2 compatibility for old/future dates.
spark.conf.set(
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite",
    "LEGACY"
)


print(
    "Writing:",
    DIM_CUSTOMER_PATH
)

(
    dim_customer
    .write
    .mode("overwrite")
    .parquet(
        DIM_CUSTOMER_PATH
    )
)


print(
    "Writing:",
    FACT_ACTIVITY_PATH
)

(
    fact_customer_activity
    .write
    .mode("overwrite")
    .parquet(
        FACT_ACTIVITY_PATH
    )
)


print(
    "Writing:",
    MONTHLY_CHURN_PATH
)

(
    monthly_churn
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(
        MONTHLY_CHURN_PATH
    )
)


# ============================================================
# STEP 15 - RELOAD GOLD
# ============================================================

log("STEP 15 - Reloading Gold for Verification")


spark.catalog.clearCache()


dim_customer_gold = spark.read.parquet(
    DIM_CUSTOMER_PATH
)

fact_activity_gold = spark.read.parquet(
    FACT_ACTIVITY_PATH
)

monthly_churn_gold = spark.read.parquet(
    MONTHLY_CHURN_PATH
)


# ============================================================
# STEP 16 - ROW COUNTS
# ============================================================

log("STEP 16 - Gold Row Counts")


dim_count = (
    dim_customer_gold.count()
)

fact_count = (
    fact_activity_gold.count()
)

churn_count = (
    monthly_churn_gold.count()
)


print()
print("ROW COUNTS")
print(
    "Dim Customer:",
    dim_count
)
print(
    "Fact Customer Activity:",
    fact_count
)
print(
    "Monthly Churn:",
    churn_count
)


# ============================================================
# STEP 17 - GOLD DATA QUALITY CHECKS
# ============================================================

log("STEP 17 - Gold Data Quality Checks")


# ------------------------------------------------------------
# Dimension keys
# ------------------------------------------------------------

null_cust_keys = (
    dim_customer_gold
    .filter(
        F.col("cust_key").isNull()
    )
    .count()
)


duplicate_cust_keys = (
    dim_customer_gold
    .groupBy("cust_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


duplicate_customers = (
    dim_customer_gold
    .groupBy("customer_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


# ------------------------------------------------------------
# Fact keys
# ------------------------------------------------------------

null_activity_keys = (
    fact_activity_gold
    .filter(
        F.col("activity_key").isNull()
    )
    .count()
)


duplicate_activity_keys = (
    fact_activity_gold
    .groupBy("activity_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


duplicate_customer_month = (
    fact_activity_gold
    .groupBy(
        "customer_id",
        "activity_month"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


# ------------------------------------------------------------
# Customer + surrogate key consistency
# ------------------------------------------------------------

null_fact_cust_keys = (
    fact_activity_gold
    .filter(
        F.col("cust_key").isNull()
    )
    .count()
)


# ------------------------------------------------------------
# Foreign key integrity
# ------------------------------------------------------------

orphan_fact_rows = (
    fact_activity_gold
    .select("cust_key")
    .distinct()
    .join(
        dim_customer_gold.select(
            "cust_key"
        ).distinct(),
        on="cust_key",
        how="left_anti"
    )
    .count()
)


# ------------------------------------------------------------
# Missing customer IDs in fact
# ------------------------------------------------------------

null_fact_customer_ids = (
    fact_activity_gold
    .filter(
        F.col("customer_id").isNull()
    )
    .count()
)


# ------------------------------------------------------------
# Activity month validity
# ------------------------------------------------------------

invalid_activity_month = (
    fact_activity_gold
    .filter(
        F.col("activity_month").isNull()
        |
        ~F.col("activity_month").rlike(
            r"^\d{4}-(0[1-9]|1[0-2])$"
        )
    )
    .count()
)


# ------------------------------------------------------------
# Acceptance rate
# ------------------------------------------------------------

invalid_acceptance_rate = (
    fact_activity_gold
    .filter(
        (F.col("acceptance_rate") < 0)
        |
        (F.col("acceptance_rate") > 1)
    )
    .count()
)


# ------------------------------------------------------------
# Churn rate
# ------------------------------------------------------------

invalid_churn_rate = (
    monthly_churn_gold
    .filter(
        (F.col("churn_rate") < 0)
        |
        (F.col("churn_rate") > 1)
    )
    .count()
)


# ------------------------------------------------------------
# Negative measures
# ------------------------------------------------------------

negative_usage = (
    fact_activity_gold
    .filter(
        F.col("usage_events") < 0
    )
    .count()
)


negative_balance = (
    fact_activity_gold
    .filter(
        F.col("total_balance") < 0
    )
    .count()
)


negative_tickets = (
    fact_activity_gold
    .filter(
        F.col("ticket_count") < 0
    )
    .count()
)


negative_offers = (
    fact_activity_gold
    .filter(
        F.col("offer_count") < 0
    )
    .count()
)


negative_accepted = (
    fact_activity_gold
    .filter(
        F.col("accepted_offers") < 0
    )
    .count()
)


# ------------------------------------------------------------
# Offer consistency
# ------------------------------------------------------------

invalid_offers = (
    fact_activity_gold
    .filter(
        F.col("accepted_offers")
        >
        F.col("offer_count")
    )
    .count()
)


# ------------------------------------------------------------
# Churn consistency
# ------------------------------------------------------------

invalid_churn_counts = (
    monthly_churn_gold
    .filter(
        F.col("churned_customers")
        >
        F.col("active_customers")
    )
    .count()
)


# ------------------------------------------------------------
# Dimension data quality
# ------------------------------------------------------------

null_customer_ids = (
    dim_customer_gold
    .filter(
        F.col("customer_id").isNull()
    )
    .count()
)


invalid_churn_flag = (
    dim_customer_gold
    .filter(
        ~F.col("is_churned").isin(
            0,
            1
        )
        |
        F.col("is_churned").isNull()
    )
    .count()
)


negative_clv = (
    dim_customer_gold
    .filter(
        F.col("CLV_LTV") < 0
    )
    .count()
)


negative_ticket_avg = (
    dim_customer_gold
    .filter(
        F.col(
            "avg_ticket_res_time_hrs"
        ) < 0
    )
    .count()
)


# ============================================================
# STEP 18 - PRINT QUALITY REPORT
# ============================================================

print()
print("==========================================================")
print("                 GOLD DATA QUALITY REPORT")
print("==========================================================")


print()
print("DIM CUSTOMER")
print(
    "Rows:",
    dim_count
)
print(
    "NULL customer_id:",
    null_customer_ids
)
print(
    "NULL cust_key:",
    null_cust_keys
)
print(
    "Duplicate cust_key:",
    duplicate_cust_keys
)
print(
    "Duplicate customer_id:",
    duplicate_customers
)
print(
    "Invalid is_churned:",
    invalid_churn_flag
)
print(
    "Negative CLV_LTV:",
    negative_clv
)
print(
    "Negative avg ticket resolution:",
    negative_ticket_avg
)


print()
print("FACT CUSTOMER ACTIVITY")
print(
    "Rows:",
    fact_count
)
print(
    "NULL activity_key:",
    null_activity_keys
)
print(
    "Duplicate activity_key:",
    duplicate_activity_keys
)
print(
    "Duplicate customer + month:",
    duplicate_customer_month
)
print(
    "NULL customer_id:",
    null_fact_customer_ids
)
print(
    "NULL cust_key:",
    null_fact_cust_keys
)
print(
    "Orphan fact rows:",
    orphan_fact_rows
)
print(
    "Invalid activity month:",
    invalid_activity_month
)


print()
print("MEASURE CHECKS")
print(
    "Negative usage_events:",
    negative_usage
)
print(
    "Negative total_balance:",
    negative_balance
)
print(
    "Negative ticket_count:",
    negative_tickets
)
print(
    "Negative offer_count:",
    negative_offers
)
print(
    "Negative accepted_offers:",
    negative_accepted
)


print()
print("RATE CHECKS")
print(
    "Invalid acceptance_rate:",
    invalid_acceptance_rate
)
print(
    "Invalid churn_rate:",
    invalid_churn_rate
)


print()
print("BUSINESS CONSISTENCY")
print(
    "Accepted offers > offer count:",
    invalid_offers
)
print(
    "Churned > active:",
    invalid_churn_counts
)


# ============================================================
# STEP 19 - FINAL STATUS
# ============================================================

gold_pass = (

    # Dimension
    null_customer_ids == 0
    and null_cust_keys == 0
    and duplicate_cust_keys == 0
    and duplicate_customers == 0
    and invalid_churn_flag == 0
    and negative_clv == 0
    and negative_ticket_avg == 0

    # Fact keys
    and null_activity_keys == 0
    and duplicate_activity_keys == 0
    and duplicate_customer_month == 0
    and null_fact_customer_ids == 0
    and null_fact_cust_keys == 0
    and orphan_fact_rows == 0

    # Activity
    and invalid_activity_month == 0

    # Measures
    and negative_usage == 0
    and negative_balance == 0
    and negative_tickets == 0
    and negative_offers == 0
    and negative_accepted == 0

    # Rates
    and invalid_acceptance_rate == 0
    and invalid_churn_rate == 0

    # Business rules
    and invalid_offers == 0
    and invalid_churn_counts == 0
)


# ============================================================
# STEP 20 - SHOW GOLD SAMPLES
# ============================================================

log("STEP 20 - Gold Samples")


print()
print("---- DIM CUSTOMER ----")

dim_customer_gold.show(
    5,
    truncate=False
)


print()
print("---- FACT CUSTOMER ACTIVITY ----")

fact_activity_gold.show(
    5,
    truncate=False
)


print()
print("---- MONTHLY CHURN RATE ----")

monthly_churn_gold.show(
    50,
    truncate=False
)


# ============================================================
# STEP 21 - GOLD HDFS VERIFICATION
# ============================================================

log("STEP 21 - Gold HDFS Verification")


for path in [
    DIM_CUSTOMER_PATH,
    FACT_ACTIVITY_PATH,
    MONTHLY_CHURN_PATH
]:

    print()
    print(
        "Checking:",
        path
    )

    if not hdfs_exists(path):

        print(
            "EXISTS: NO"
        )

        continue

    print(
        "EXISTS: YES"
    )

    fs = get_fs()

    statuses = fs.listStatus(
        spark._jvm.org.apache.hadoop.fs.Path(
            path
        )
    )

    for status in statuses:

        print(
            " -",
            status.getPath().toString()
        )


# ============================================================
# FINAL STATUS
# ============================================================

print()
print("=" * 70)

if gold_pass:

    print(
        "              GOLD LAYER STATUS: PASS"
    )

else:

    print(
        "             GOLD LAYER STATUS: CHECK"
    )

print("=" * 70)


print()
print("Gold outputs:")

print(
    " ",
    DIM_CUSTOMER_PATH
)

print(
    " ",
    FACT_ACTIVITY_PATH
)

print(
    " ",
    MONTHLY_CHURN_PATH
)

print()
print(
    "Next stage:"
)
print(
    "Gold -> Hive -> BI / Power BI / Analytics"
)


spark.stop()


if not gold_pass:

    sys.exit(1)
