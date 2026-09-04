# ============================================================
# BRONZE TO SILVER CLEANING PIPELINE
# ============================================================
#
# Purpose:
#     Clean, validate, deduplicate, and transform the four
#     Bronze data sources into trusted Silver datasets.
#
# Data flow:
#     Bronze
#        |
#        +--> Customers
#        |
#        +--> Customer Support Tickets
#        |
#        +--> Offers
#        |
#        +--> Usage
#        |
#        v
#     Silver
#
# Processing order:
#     1. Customers
#     2. Customer Support Tickets
#     3. Offers
#     4. Usage
#
# Customers are processed first because Tickets and Usage use
# the final clean CustomerId dataset for foreign-key validation.
#
# Each source follows four stages:
#
#     Stage 1 - Standardization and Type Casting
#         - Trim whitespace
#         - Normalize text values
#         - Convert columns to the correct data types
#         - Normalize date formats
#
#     Stage 2 - Exact Duplicate Removal
#         - Detect completely identical records
#         - Keep the first record
#         - Send duplicate records to the rejected dataset
#
#     Stage 3 - Data Validation
#         - Check required fields
#         - Check valid ranges and allowed values
#         - Check date validity
#         - Check foreign keys where required
#
#     Stage 4 - Business-Key Deduplication and Final Write
#         - Enforce uniqueness of business keys
#         - Write valid records to Silver
#         - Write rejected records with a rejection reason
#         - Produce a reconciliation report
#
# Output:
#     /data/silver/customers
#     /data/silver/customer_support_tickets
#     /data/silver/offers
#     /data/silver/usage
#
# Rejected records:
#     /data/silver/_rejected/<source>
#
# Staging data:
#     /data/_staging/<source>
#
# Customer foreign-key reference:
#     /data/silver/_reference/customer_ids
#
# Important:
#     The CustomerId reference is created from the FINAL clean
#     Customers Silver dataset. Therefore, Tickets and Usage
#     cannot pass the FK check using invalid or rejected customers.
#
# Reliability:
#     Each major stage is written to Parquet and reloaded to
#     break Spark lineage and reduce driver memory pressure.
#
# Final reconciliation:
#     Source Rows = Clean Rows + Rejected Rows
#
#     Any unaccounted rows cause the pipeline to fail instead
#     of silently losing data.
#
# ============================================================

#!/usr/bin/env python3

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    initcap,
    regexp_replace,
    regexp_extract,
    concat,
    to_date,
    coalesce,
    current_date,
    date_format,
    lit,
    when,
    monotonically_increasing_id,
    row_number,
    sha2,
    struct,
    to_json,
    broadcast,
)
from pyspark.sql.window import Window


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("Bronze to Silver - All Sources")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HDFS PATHS
# ============================================================

STAGING_ROOT = "hdfs:///data/_staging"
SILVER_ROOT = "hdfs:///data/silver"

# This contains only CustomerIds that survived:
# 1. customer validation
# 2. CustomerId business-key deduplication
CUSTOMER_IDS_REF = f"{SILVER_ROOT}/_reference/customer_ids"


# ============================================================
# SHARED FUNCTIONS
# ============================================================

def write_and_reload(df, path):
    """
    Write Parquet and read it again.

    This breaks the Spark lineage and helps prevent a very
    large logical plan from building in the driver.
    """
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def hash_dedupe(df, columns, order_col):
    """
    Remove exact duplicate rows.

    A JSON representation of an explicit struct is hashed.
    This is safer than concat_ws() because column names and
    NULL values cannot become ambiguous across columns.
    """

    row_json = to_json(
        struct(*[col(c) for c in columns])
    )

    hashed = df.withColumn(
        "_row_hash",
        sha2(row_json, 256)
    )

    window_spec = (
        Window
        .partitionBy("_row_hash")
        .orderBy(col(order_col))
    )

    marked = hashed.withColumn(
        "_dup_number",
        row_number().over(window_spec)
    )

    exact_dupes = (
        marked
        .filter(col("_dup_number") > 1)
        .withColumn(
            "rejection_reason",
            lit("exact_duplicate")
        )
        .select(*columns, "rejection_reason")
    )

    unique_df = (
        marked
        .filter(col("_dup_number") == 1)
        .drop("_dup_number", "_row_hash")
    )

    return unique_df, exact_dupes


def business_key_dedupe(
    df,
    columns,
    key_cols,
    order_col,
    reason_label,
):
    """
    Keep one row for each business key.

    Remaining duplicates are sent to the rejected dataset.
    """

    window_spec = (
        Window
        .partitionBy(*key_cols)
        .orderBy(col(order_col))
    )

    marked = df.withColumn(
        "_n",
        row_number().over(window_spec)
    )

    clean_df = (
        marked
        .filter(col("_n") == 1)
        .select(*columns)
    )

    dup_df = (
        marked
        .filter(col("_n") > 1)
        .withColumn(
            "rejection_reason",
            lit(reason_label)
        )
        .select(*columns, "rejection_reason")
    )

    return clean_df, dup_df


def add_fk_check(df, key="customer_id"):
    """
    Check whether the CustomerId exists in the FINAL clean
    Customers Silver reference.
    """

    customer_ids = (
        spark.read
        .parquet(CUSTOMER_IDS_REF)
        .select(
            col("CustomerId").alias(key)
        )
        .distinct()
        .withColumn(
            "_customer_exists",
            lit(True)
        )
    )

    return df.join(
        broadcast(customer_ids),
        on=key,
        how="left",
    )


def print_report(
    name,
    source_count,
    clean_count,
    rejected_count,
    rejected_df,
):
    """
    Print the final reconciliation report.
    """

    unaccounted = (
        source_count
        - clean_count
        - rejected_count
    )

    rejection_rate = (
        rejected_count / source_count * 100
        if source_count > 0
        else 0
    )

    print("=" * 70)
    print(f"FINAL REPORT - {name.upper()}")
    print("=" * 70)
    print("Source rows      :", source_count)
    print("Clean rows       :", clean_count)
    print("Rejected rows    :", rejected_count)
    print("Unaccounted rows :", unaccounted)
    print(
        "Rejection rate   :",
        round(rejection_rate, 2),
        "%",
    )
    print("=" * 70)

    (
        rejected_df
        .groupBy("rejection_reason")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    if unaccounted != 0:
        raise RuntimeError(
            f"{name}: record reconciliation failed. "
            f"Unaccounted rows = {unaccounted}"
        )


# ============================================================
# 1. CUSTOMERS
# ============================================================

def clean_customers():

    NAME = "customers"

    BRONZE = (
        "hdfs:///data/bronze/customers"
    )

    STAGE = (
        f"{STAGING_ROOT}/{NAME}"
    )

    CLEAN_PATH = (
        f"{SILVER_ROOT}/{NAME}"
    )

    REJECTED_PATH = (
        f"{SILVER_ROOT}/_rejected/{NAME}"
    )

    columns = [
        "RowNumber",
        "CustomerId",
        "Surname",
        "CreditScore",
        "Geography",
        "Gender",
        "Age",
        "Tenure",
        "Balance",
        "NumOfProducts",
        "HasCrCard",
        "IsActiveMember",
        "EstimatedSalary",
        "Exited",
        "tenure_months",
        "date_opened",
    ]

    # --------------------------------------------------------
    # Stage 1: Standardize + Cast
    # --------------------------------------------------------

    df = (
        spark.read
        .option("header", "false")
        .csv(BRONZE)
        .toDF(*columns)
        .withColumn(
            "_source_id",
            monotonically_increasing_id()
        )
    )

    exprs = []

    for c in columns:

        raw = trim(col(c))

        raw = when(
            raw == "",
            None,
        ).otherwise(raw)

        if c in ("Geography", "Gender"):

            raw = initcap(raw)

        if c in ("Balance", "EstimatedSalary"):

            raw = (
                regexp_replace(
                    raw,
                    "[$,]",
                    "",
                )
                .cast("double")
            )

        elif c == "RowNumber":

            raw = raw.cast("int")

        elif c == "CustomerId":

            raw = raw.cast("long")

        elif c == "CreditScore":

            raw = raw.cast("double")

        elif c in (
            "Age",
            "Tenure",
            "NumOfProducts",
            "HasCrCard",
            "IsActiveMember",
            "Exited",
            "tenure_months",
        ):

            raw = raw.cast("int")

        elif c == "date_opened":

            raw = coalesce(
                to_date(
                    raw,
                    "yyyy-MM-dd",
                ),
                to_date(
                    raw,
                    "yyyy/MM/dd",
                ),
                to_date(
                    raw,
                    "dd-MM-yyyy",
                ),
                to_date(
                    raw,
                    "MM/dd/yyyy",
                ),
                to_date(
                    raw,
                    "dd MMM yyyy",
                ),
            )

        exprs.append(
            raw.alias(c)
        )

    exprs.append(
        col("_source_id")
    )

    df = write_and_reload(
        df.select(*exprs),
        f"{STAGE}/stage1_standardized",
    )

    source_count = df.count()

    print(
        f"[{NAME}] Stage 1 done. "
        f"rows = {source_count}"
    )

    # --------------------------------------------------------
    # Stage 2: Exact duplicates
    # --------------------------------------------------------

    unique_df, exact_dupes = hash_dedupe(
        df,
        columns,
        "_source_id",
    )

    unique_df = write_and_reload(
        unique_df,
        f"{STAGE}/stage2_deduped",
    )

    exact_dupes = write_and_reload(
        exact_dupes,
        f"{STAGE}/stage2_rejected_exact",
    )

    unique_count = unique_df.count()
    exact_count = exact_dupes.count()

    print(
        f"[{NAME}] Stage 2 done. "
        f"unique = {unique_count} | "
        f"exact_dupes = {exact_count}"
    )

    # --------------------------------------------------------
    # Stage 3: Validation
    # --------------------------------------------------------

    valid_condition = (
        col("CustomerId").isNotNull()

        & col("CreditScore").isNotNull()
        & col("CreditScore").between(
            300,
            850,
        )

        & col("Surname").isNotNull()

        & col("Geography").isNotNull()
        & col("Geography").isin(
            "France",
            "Spain",
            "Germany",
        )

        & col("Gender").isNotNull()
        & col("Gender").isin(
            "Male",
            "Female",
        )

        & col("Age").isNotNull()
        & col("Age").between(
            18,
            90,
        )

        & col("Tenure").isNotNull()
        & (col("Tenure") >= 0)
        & (col("Tenure") < col("Age"))

        & col("Balance").isNotNull()
        & (col("Balance") >= 0)

        & col("NumOfProducts").isNotNull()
        & (col("NumOfProducts") >= 0)

        & col("HasCrCard").isNotNull()
        & col("HasCrCard").isin(
            0,
            1,
        )

        & col("IsActiveMember").isNotNull()
        & col("IsActiveMember").isin(
            0,
            1,
        )

        & col("EstimatedSalary").isNotNull()
        & (col("EstimatedSalary") >= 0)

        & col("Exited").isNotNull()
        & col("Exited").isin(
            0,
            1,
        )

        & col("tenure_months").isNotNull()
        & (col("tenure_months") >= 0)

        & col("date_opened").isNotNull()
        & (
            col("date_opened")
            <= current_date()
        )
    )

    reason = (
        when(
            col("CustomerId").isNull(),
            "missing_customer_id",
        )

        .when(
            col("CreditScore").isNull()
            | ~col("CreditScore").between(
                300,
                850,
            ),
            "invalid_credit_score",
        )

        .when(
            col("Surname").isNull(),
            "missing_surname",
        )

        .when(
            col("Geography").isNull()
            | ~col("Geography").isin(
                "France",
                "Spain",
                "Germany",
            ),
            "invalid_geography",
        )

        .when(
            col("Gender").isNull()
            | ~col("Gender").isin(
                "Male",
                "Female",
            ),
            "invalid_gender",
        )

        .when(
            col("Age").isNull()
            | ~col("Age").between(
                18,
                90,
            ),
            "invalid_age",
        )

        .when(
            col("Tenure").isNull()
            | (col("Tenure") < 0)
            | (col("Tenure") >= col("Age")),
            "invalid_tenure",
        )

        .when(
            col("Balance").isNull()
            | (col("Balance") < 0),
            "invalid_balance",
        )

        .when(
            col("NumOfProducts").isNull()
            | (col("NumOfProducts") < 0),
            "invalid_num_products",
        )

        .when(
            col("HasCrCard").isNull()
            | ~col("HasCrCard").isin(
                0,
                1,
            ),
            "invalid_has_cr_card",
        )

        .when(
            col("IsActiveMember").isNull()
            | ~col("IsActiveMember").isin(
                0,
                1,
            ),
            "invalid_active_member",
        )

        .when(
            col("EstimatedSalary").isNull()
            | (col("EstimatedSalary") < 0),
            "invalid_estimated_salary",
        )

        .when(
            col("Exited").isNull()
            | ~col("Exited").isin(
                0,
                1,
            ),
            "invalid_exited",
        )

        .when(
            col("tenure_months").isNull()
            | (col("tenure_months") < 0),
            "invalid_tenure_months",
        )

        .when(
            col("date_opened").isNull()
            | (
                col("date_opened")
                > current_date()
            ),
            "invalid_date_opened",
        )

        .otherwise("invalid_record")
    )

    flagged = (
        unique_df
        .withColumn(
            "_is_valid",
            valid_condition,
        )
        .withColumn(
            "_reason",
            reason,
        )
    )

    valid_df = (
        flagged
        .filter(col("_is_valid"))
        .select(
            *columns,
            "_source_id",
        )
    )

    invalid_df = (
        flagged
        .filter(~col("_is_valid"))
        .withColumnRenamed(
            "_reason",
            "rejection_reason",
        )
        .select(
            *columns,
            "rejection_reason",
        )
    )

    valid_df = write_and_reload(
        valid_df,
        f"{STAGE}/stage3_valid",
    )

    invalid_df = write_and_reload(
        invalid_df,
        f"{STAGE}/stage3_rejected",
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(
        f"[{NAME}] Stage 3 done. "
        f"valid = {valid_count} | "
        f"invalid = {invalid_count}"
    )

    # --------------------------------------------------------
    # Stage 4: CustomerId dedupe
    # --------------------------------------------------------

    # Use the original unique dataframe with the validation
    # condition so _source_id is available as a tie-breaker.
    stage3_with_source = (
        unique_df
        .filter(valid_condition)
    )

    clean_df, dup_df = business_key_dedupe(
        stage3_with_source,
        columns,
        ["CustomerId"],
        "_source_id",
        "duplicate_customer_id",
    )

    rejected_df = (
        invalid_df
        .unionByName(exact_dupes)
        .unionByName(dup_df)
    )

    clean_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(CLEAN_PATH)

    rejected_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(REJECTED_PATH)

    clean_count = (
        spark.read
        .parquet(CLEAN_PATH)
        .count()
    )

    rejected_count = (
        spark.read
        .parquet(REJECTED_PATH)
        .count()
    )

    # IMPORTANT:
    # Build the FK reference only from FINAL clean customers.
    (
        spark.read
        .parquet(CLEAN_PATH)
        .select("CustomerId")
        .filter(
            col("CustomerId").isNotNull()
        )
        .distinct()
        .write.mode("overwrite")
        .parquet(CUSTOMER_IDS_REF)
    )

    print(
        f"[{NAME}] Final customer ID reference "
        f"written to {CUSTOMER_IDS_REF}"
    )

    print_report(
        NAME,
        source_count,
        clean_count,
        rejected_count,
        spark.read.parquet(
            REJECTED_PATH
        ),
    )


# ============================================================
# 2. CUSTOMER SUPPORT TICKETS
# ============================================================

def clean_tickets():

    NAME = "customer_support_tickets"

    BRONZE = (
        "hdfs:///data/bronze/"
        "customer_support_tickets/"
        "Customer_support_tickets.csv"
    )

    STAGE = f"{STAGING_ROOT}/tickets"

    CLEAN_PATH = (
        f"{SILVER_ROOT}/{NAME}"
    )

    REJECTED_PATH = (
        f"{SILVER_ROOT}/_rejected/{NAME}"
    )

    columns = [
        "ticket_id",
        "customer_id",
        "issue_type",
        "severity",
        "resolution_time_hrs",
        "created_at",
    ]

    VALID_ISSUE_TYPES = [
        "app_bug",
        "fee_dispute",
        "card_decline",
        "fraud_alert",
        "transfer_delay",
        "login_issue",
    ]

    VALID_SEVERITIES = [
        "low",
        "medium",
        "high",
        "critical",
    ]

    # --------------------------------------------------------
    # Stage 1
    # --------------------------------------------------------

    df = (
        spark.read
        .option("header", "true")
        .csv(BRONZE)
        .toDF(*columns)
        .withColumn(
            "_source_id",
            monotonically_increasing_id(),
        )
    )

    exprs = []

    for c in columns:

        raw = trim(col(c))

        raw = when(
            raw == "",
            None,
        ).otherwise(raw)

        if c in (
            "issue_type",
            "severity",
        ):

            raw = lower(raw)

        elif c == "customer_id":

            raw = raw.cast("long")

        elif c == "resolution_time_hrs":

            raw = raw.cast("double")

        elif c == "created_at":

            raw = coalesce(
                to_date(
                    raw,
                    "yyyy-MM-dd",
                ),
                to_date(
                    raw,
                    "yyyy/MM/dd",
                ),
                to_date(
                    raw,
                    "dd-MM-yyyy",
                ),
                to_date(
                    raw,
                    "MM/dd/yyyy",
                ),
                to_date(
                    raw,
                    "dd MMM yyyy",
                ),
            )

        exprs.append(
            raw.alias(c)
        )

    exprs.append(
        col("_source_id")
    )

    df = write_and_reload(
        df.select(*exprs),
        f"{STAGE}/stage1_standardized",
    )

    source_count = df.count()

    print(
        f"[{NAME}] Stage 1 done. "
        f"rows = {source_count}"
    )

    # --------------------------------------------------------
    # Stage 2
    # --------------------------------------------------------

    unique_df, exact_dupes = hash_dedupe(
        df,
        columns,
        "_source_id",
    )

    unique_df = write_and_reload(
        unique_df,
        f"{STAGE}/stage2_deduped",
    )

    exact_dupes = write_and_reload(
        exact_dupes,
        f"{STAGE}/stage2_rejected_exact",
    )

    print(
        f"[{NAME}] Stage 2 done. "
        f"unique = {unique_df.count()} | "
        f"exact_dupes = {exact_dupes.count()}"
    )

    # --------------------------------------------------------
    # Stage 3: Validation + FK
    # --------------------------------------------------------

    checked = add_fk_check(
        unique_df,
        "customer_id",
    )

    valid_condition = (
        col("ticket_id").isNotNull()

        & col("customer_id").isNotNull()

        & col("_customer_exists").isNotNull()

        & col("issue_type").isNotNull()
        & col("issue_type").isin(
            *VALID_ISSUE_TYPES
        )

        & col("severity").isNotNull()
        & col("severity").isin(
            *VALID_SEVERITIES
        )

        & col(
            "resolution_time_hrs"
        ).isNotNull()

        & (
            col("resolution_time_hrs")
            >= 0
        )

        & (
            col("resolution_time_hrs")
            <= 720
        )

        & col("created_at").isNotNull()

        & (
            col("created_at")
            <= current_date()
        )
    )

    reason = (
        when(
            col("ticket_id").isNull(),
            "missing_ticket_id",
        )

        .when(
            col("customer_id").isNull(),
            "missing_customer_id",
        )

        .when(
            col("_customer_exists").isNull(),
            "orphan_customer_id",
        )

        .when(
            col("issue_type").isNull()
            | ~col("issue_type").isin(
                *VALID_ISSUE_TYPES
            ),
            "invalid_issue_type",
        )

        .when(
            col("severity").isNull()
            | ~col("severity").isin(
                *VALID_SEVERITIES
            ),
            "invalid_severity",
        )

        .when(
            col(
                "resolution_time_hrs"
            ).isNull()
            | (
                col("resolution_time_hrs")
                < 0
            )
            | (
                col("resolution_time_hrs")
                > 720
            ),
            "invalid_resolution_time",
        )

        .when(
            col("created_at").isNull()
            | (
                col("created_at")
                > current_date()
            ),
            "invalid_created_at",
        )

        .otherwise(
            "invalid_record"
        )
    )

    flagged = (
        checked
        .withColumn(
            "_is_valid",
            valid_condition,
        )
        .withColumn(
            "_reason",
            reason,
        )
    )

    valid_df = (
        flagged
        .filter(col("_is_valid"))
        .select(
            *columns,
            "_source_id",
        )
    )

    invalid_df = (
        flagged
        .filter(~col("_is_valid"))
        .withColumnRenamed(
            "_reason",
            "rejection_reason",
        )
        .select(
            *columns,
            "rejection_reason",
        )
    )

    valid_df = write_and_reload(
        valid_df,
        f"{STAGE}/stage3_valid",
    )

    invalid_df = write_and_reload(
        invalid_df,
        f"{STAGE}/stage3_rejected",
    )

    print(
        f"[{NAME}] Stage 3 done. "
        f"valid = {valid_df.count()} | "
        f"invalid = {invalid_df.count()}"
    )

    # --------------------------------------------------------
    # Stage 4
    # --------------------------------------------------------

    clean_df, dup_df = business_key_dedupe(
        valid_df,
        columns,
        ["ticket_id"],
        "_source_id",
        "duplicate_ticket_id",
    )

    rejected_df = (
        invalid_df
        .unionByName(exact_dupes)
        .unionByName(dup_df)
    )

    clean_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(CLEAN_PATH)

    rejected_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(REJECTED_PATH)

    clean_count = (
        spark.read
        .parquet(CLEAN_PATH)
        .count()
    )

    rejected_count = (
        spark.read
        .parquet(REJECTED_PATH)
        .count()
    )

    print_report(
        NAME,
        source_count,
        clean_count,
        rejected_count,
        spark.read.parquet(
            REJECTED_PATH
        ),
    )


# ============================================================
# 3. OFFERS
# ============================================================

def clean_offers():

    NAME = "offers"

    BRONZE = (
        "hdfs:///data/bronze/"
        "offers/Offers.csv"
    )

    STAGE = f"{STAGING_ROOT}/{NAME}"

    CLEAN_PATH = (
        f"{SILVER_ROOT}/{NAME}"
    )

    REJECTED_PATH = (
        f"{SILVER_ROOT}/_rejected/{NAME}"
    )

    columns = [
        "offer_id",
        "customer_id",
        "offer_type",
        "accepted",
        "date_offered",
    ]

    VALID_OFFER_TYPES = [
        "card_upgrade",
        "fee_waiver",
        "loan_rate_cut",
        "savings_bonus",
        "cashback",
    ]

    # --------------------------------------------------------
    # Stage 1
    # --------------------------------------------------------

    df = (
        spark.read
        .option("header", "true")
        .csv(BRONZE)
        .toDF(*columns)
        .withColumn(
            "_source_id",
            monotonically_increasing_id(),
        )
    )

    exprs = []

    for c in columns:

        raw = trim(col(c))

        raw = when(
            raw == "",
            None,
        ).otherwise(raw)

        if c == "offer_type":

            raw = lower(raw)

        elif c == "customer_id":

            raw = raw.cast("long")

        elif c == "date_offered":

            raw = coalesce(
                to_date(
                    raw,
                    "yyyy-MM-dd",
                ),
                to_date(
                    raw,
                    "yyyy/MM/dd",
                ),
                to_date(
                    raw,
                    "dd-MM-yyyy",
                ),
                to_date(
                    raw,
                    "MM/dd/yyyy",
                ),
                to_date(
                    raw,
                    "dd MMM yyyy",
                ),
            )

        elif c == "accepted":

            low = lower(raw)

            raw = (
                when(
                    low.isin(
                        "1",
                        "yes",
                        "y",
                        "true",
                    ),
                    1,
                )
                .when(
                    low.isin(
                        "0",
                        "no",
                        "n",
                        "false",
                    ),
                    0,
                )
                .otherwise(None)
                .cast("int")
            )

        exprs.append(
            raw.alias(c)
        )

    exprs.append(
        col("_source_id")
    )

    df = write_and_reload(
        df.select(*exprs),
        f"{STAGE}/stage1_standardized",
    )

    source_count = df.count()

    print(
        f"[{NAME}] Stage 1 done. "
        f"rows = {source_count}"
    )

    # --------------------------------------------------------
    # Stage 2
    # --------------------------------------------------------

    unique_df, exact_dupes = hash_dedupe(
        df,
        columns,
        "_source_id",
    )

    unique_df = write_and_reload(
        unique_df,
        f"{STAGE}/stage2_deduped",
    )

    exact_dupes = write_and_reload(
        exact_dupes,
        f"{STAGE}/stage2_rejected_exact",
    )

    print(
        f"[{NAME}] Stage 2 done. "
        f"unique = {unique_df.count()} | "
        f"exact_dupes = {exact_dupes.count()}"
    )

    # --------------------------------------------------------
    # Stage 3: Validation
    # --------------------------------------------------------
    # No FK check here because the original requirement
    # explicitly did not require it for Offers.

    valid_condition = (
        col("offer_id").isNotNull()

        & col("customer_id").isNotNull()

        & col("offer_type").isNotNull()
        & col("offer_type").isin(
            *VALID_OFFER_TYPES
        )

        & col("accepted").isNotNull()
        & col("accepted").isin(
            0,
            1,
        )

        & col("date_offered").isNotNull()

        & (
            col("date_offered")
            <= current_date()
        )
    )

    reason = (
        when(
            col("offer_id").isNull(),
            "missing_offer_id",
        )

        .when(
            col("customer_id").isNull(),
            "missing_customer_id",
        )

        .when(
            col("offer_type").isNull()
            | ~col("offer_type").isin(
                *VALID_OFFER_TYPES
            ),
            "invalid_offer_type",
        )

        .when(
            col("accepted").isNull()
            | ~col("accepted").isin(
                0,
                1,
            ),
            "invalid_accepted",
        )

        .when(
            col("date_offered").isNull()
            | (
                col("date_offered")
                > current_date()
            ),
            "invalid_date_offered",
        )

        .otherwise(
            "invalid_record"
        )
    )

    flagged = (
        unique_df
        .withColumn(
            "_is_valid",
            valid_condition,
        )
        .withColumn(
            "_reason",
            reason,
        )
    )

    valid_df = (
        flagged
        .filter(col("_is_valid"))
        .select(
            *columns,
            "_source_id",
        )
    )

    invalid_df = (
        flagged
        .filter(~col("_is_valid"))
        .withColumnRenamed(
            "_reason",
            "rejection_reason",
        )
        .select(
            *columns,
            "rejection_reason",
        )
    )

    valid_df = write_and_reload(
        valid_df,
        f"{STAGE}/stage3_valid",
    )

    invalid_df = write_and_reload(
        invalid_df,
        f"{STAGE}/stage3_rejected",
    )

    print(
        f"[{NAME}] Stage 3 done. "
        f"valid = {valid_df.count()} | "
        f"invalid = {invalid_df.count()}"
    )

    # --------------------------------------------------------
    # Stage 4
    # --------------------------------------------------------

    clean_df, dup_df = business_key_dedupe(
        valid_df,
        columns,
        ["offer_id"],
        "_source_id",
        "duplicate_offer_id",
    )

    rejected_df = (
        invalid_df
        .unionByName(exact_dupes)
        .unionByName(dup_df)
    )

    clean_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(CLEAN_PATH)

    rejected_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(REJECTED_PATH)

    clean_count = (
        spark.read
        .parquet(CLEAN_PATH)
        .count()
    )

    rejected_count = (
        spark.read
        .parquet(REJECTED_PATH)
        .count()
    )

    print_report(
        NAME,
        source_count,
        clean_count,
        rejected_count,
        spark.read.parquet(
            REJECTED_PATH
        ),
    )


# ============================================================
# 4. USAGE
# ============================================================

def clean_usage():

    NAME = "usage"

    BRONZE = (
        "hdfs:///data/bronze/"
        "usage/Usage.json"
    )

    STAGE = f"{STAGING_ROOT}/{NAME}"

    CLEAN_PATH = (
        f"{SILVER_ROOT}/{NAME}"
    )

    REJECTED_PATH = (
        f"{SILVER_ROOT}/_rejected/{NAME}"
    )

    columns = [
        "usage_log_id",
        "customer_id",
        "usage_month",
        "product_type",
        "monthly_balance",
        "num_products",
    ]

    # --------------------------------------------------------
    # Stage 1: Standardize + Cast
    # --------------------------------------------------------

    df = (
        spark.read
        .option("multiline", "true")
        .json(BRONZE)
    )

    # Make sure all expected columns exist.
    for c in columns:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))

    # Keep only the expected columns and add a unique source row ID.
    df = (
        df
        .select(*columns)
        .withColumn(
            "_source_id",
            monotonically_increasing_id()
        )
    )

    # Build all standardized expressions.
    exprs = []

    for c in columns:

        raw = trim(
            col(c).cast("string")
        )

        # Convert empty strings to NULL.
        raw = when(
            raw == "",
            None
        ).otherwise(raw)

        # Normalize product type.
        if c == "product_type":

            raw = lower(raw)

        # Numeric integer columns.
        elif c in (
                "customer_id",
                "num_products",
        ):

            raw = raw.cast("long")

        # usage_log_id is an alphanumeric string such as:
        # USG553504902
        elif c == "usage_log_id":

            raw = raw.cast("string")

        # Numeric balance.
        elif c == "monthly_balance":

            raw = (
                regexp_replace(
                    raw,
                    "[$,]",
                    ""
                )
                .cast("double")
            )

        # Standardize usage_month to YYYY-MM.
        elif c == "usage_month":

            raw = (
                when(
                    raw.rlike(
                        r"^\d{4}-(0[1-9]|1[0-2])$"
                    ),
                    raw
                )

                # Example: 2025/10 -> 2025-10
                .when(
                    raw.rlike(
                        r"^\d{4}/(0[1-9]|1[0-2])$"
                    ),
                    regexp_replace(
                        raw,
                        "/",
                        "-"
                    )
                )

                # Example: 10/2025 -> 2025-10
                .when(
                    raw.rlike(
                        r"^(0[1-9]|1[0-2])/\d{4}$"
                    ),
                    concat(
                        regexp_extract(
                            raw,
                            r"^(0[1-9]|1[0-2])/(\d{4})$",
                            2
                        ),
                        lit("-"),
                        regexp_extract(
                            raw,
                            r"^(\d{2})/\d{4}$",
                            1
                        )
                    )
                )

                .otherwise(None)
            )

        exprs.append(
            raw.alias(c)
        )

    # Keep the technical source ID for duplicate detection.
    exprs.append(
        col("_source_id")
    )

    df = write_and_reload(
        df.select(*exprs),
        f"{STAGE}/stage1_standardized"
    )

    source_count = df.count()

    print(
        f"[{NAME}] Stage 1 done. "
        f"rows = {source_count}"
    )

    # --------------------------------------------------------
    # Stage 2
    # --------------------------------------------------------

    unique_df, exact_dupes = hash_dedupe(
        df,
        columns,
        "_source_id",
    )

    unique_df = write_and_reload(
        unique_df,
        f"{STAGE}/stage2_deduped",
    )

    exact_dupes = write_and_reload(
        exact_dupes,
        f"{STAGE}/stage2_rejected_exact",
    )

    print(
        f"[{NAME}] Stage 2 done. "
        f"unique = {unique_df.count()} | "
        f"exact_dupes = {exact_dupes.count()}"
    )

    # --------------------------------------------------------
    # Stage 3: Validation + FK
    # --------------------------------------------------------

    checked = add_fk_check(
        unique_df,
        "customer_id",
    )

    current_month = date_format(
        current_date(),
        "yyyy-MM",
    )

    valid_usage_month = (
        col("usage_month").rlike(
            r"^\d{4}-(0[1-9]|1[0-2])$"
        )
    )

    valid_condition = (
        col("usage_log_id").isNotNull()

        & col("customer_id").isNotNull()

        & col("_customer_exists").isNotNull()

        & col("usage_month").isNotNull()
        & valid_usage_month

        & (
            col("usage_month")
            <= current_month
        )

        & col("product_type").isNotNull()

        & col("monthly_balance").isNotNull()
        & (
            col("monthly_balance")
            >= 0
        )

        & col("num_products").isNotNull()
        & (
            col("num_products")
            >= 0
        )
    )

    reason = (
        when(
            col("usage_log_id").isNull(),
            "missing_usage_log_id",
        )

        .when(
            col("customer_id").isNull(),
            "missing_customer_id",
        )

        .when(
            col("_customer_exists").isNull(),
            "orphan_customer_id",
        )

        .when(
            col("usage_month").isNull()
            | ~valid_usage_month
            | (
                col("usage_month")
                > current_month
            ),
            "invalid_usage_month",
        )

        .when(
            col("product_type").isNull(),
            "missing_product_type",
        )

        .when(
            col("monthly_balance").isNull()
            | (
                col("monthly_balance")
                < 0
            ),
            "invalid_monthly_balance",
        )

        .when(
            col("num_products").isNull()
            | (
                col("num_products")
                < 0
            ),
            "invalid_num_products",
        )

        .otherwise(
            "invalid_record"
        )
    )

    flagged = (
        checked
        .withColumn(
            "_is_valid",
            valid_condition,
        )
        .withColumn(
            "_reason",
            reason,
        )
    )

    valid_df = (
        flagged
        .filter(col("_is_valid"))
        .select(
            *columns,
            "_source_id",
        )
    )

    invalid_df = (
        flagged
        .filter(~col("_is_valid"))
        .withColumnRenamed(
            "_reason",
            "rejection_reason",
        )
        .select(
            *columns,
            "rejection_reason",
        )
    )

    valid_df = write_and_reload(
        valid_df,
        f"{STAGE}/stage3_valid",
    )

    invalid_df = write_and_reload(
        invalid_df,
        f"{STAGE}/stage3_rejected",
    )

    print(
        f"[{NAME}] Stage 3 done. "
        f"valid = {valid_df.count()} | "
        f"invalid = {invalid_df.count()}"
    )

    # --------------------------------------------------------
    # Stage 4
    # --------------------------------------------------------

    clean_df, dup_df = business_key_dedupe(
        valid_df,
        columns,
        ["usage_log_id"],
        "_source_id",
        "duplicate_usage_log_id",
    )

    rejected_df = (
        invalid_df
        .unionByName(exact_dupes)
        .unionByName(dup_df)
    )

    clean_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(CLEAN_PATH)

    rejected_df.coalesce(4).write.mode(
        "overwrite"
    ).parquet(REJECTED_PATH)

    clean_count = (
        spark.read
        .parquet(CLEAN_PATH)
        .count()
    )

    rejected_count = (
        spark.read
        .parquet(REJECTED_PATH)
        .count()
    )

    print_report(
        NAME,
        source_count,
        clean_count,
        rejected_count,
        spark.read.parquet(
            REJECTED_PATH
        ),
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    pipeline = [
        ("customers", clean_customers),
        ("tickets", clean_tickets),
        ("offers", clean_offers),
        ("usage", clean_usage),
    ]

    results = {}

    for name, func in pipeline:

        try:

            print("\n" + "#" * 70)
            print(f"# STARTING: {name}")
            print("#" * 70)

            func()

            results[name] = "SUCCESS"

        except Exception as e:

            print("=" * 70)
            print(f"PIPELINE FAILED - {name.upper()}")
            print("Error:", str(e))
            print("=" * 70)

            results[name] = (
                f"FAILED: {e}"
            )

            # Tickets and Usage require the final valid
            # customer reference.
            if name == "customers":

                print(
                    "Customers failed. "
                    "Stopping the remaining pipeline because "
                    "Tickets and Usage require valid customers."
                )

                break

    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)

    for name, status in results.items():

        print(
            f"{name:12s} -> {status}"
        )

    print("=" * 70)

    spark.stop()

    if any(
        str(status).startswith("FAILED")
        for status in results.values()
    ):
        sys.exit(1)
