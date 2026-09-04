# NiFi Ingestion Template

## Overview

This repository contains the Apache NiFi ingestion template used for the churn customer ETL pipeline.

The template is responsible for ingesting three source files from the Linux desktop directory into the Hadoop HDFS Bronze layer:

- `Customer_support_tickets.csv`
- `Offers.csv`
- `Usage.json`

The customer source (`Customers.csv`) is handled separately through MariaDB and Sqoop, so it is not part of this NiFi template.

## Architecture

The NiFi template follows this flow:

```text
Source Files
    |
    v
+----------------------+
|   Apache NiFi        |
|                      |
|  GetFile processors  |
|          |           |
|     LogAttribute     |
|          |           |
|      PutHDFS         |
+----------|-----------+
           |
           v
      HDFS Bronze
```

### Architecture Image

<img width="1918" height="878" alt="vmware_Iqioy7BeC8" src="https://github.com/user-attachments/assets/6fbeee94-23d8-48e5-b2ce-a3d31ba2448f" />


## Source Location

The template reads files from:

```text
/home/hadoop/Desktop
```

Each `GetFile` processor uses a filename filter so that only the expected file is processed.

| Source File | Filter | Bronze Destination |
|---|---|---|
| `Customer_support_tickets.csv` | `^Customer_support_tickets\.csv$` | `/data/bronze/customer_support_tickets` |
| `Offers.csv` | `^Offers\.csv$` | `/data/bronze/offers` |
| `Usage.json` | `^Usage\.json$` | `/data/bronze/usage` |

## NiFi Processors

The template contains nine processors:

### Customer Support Tickets

```text
GetFile
   -> LogAttribute
   -> PutHDFS
```

Input:

```text
/home/hadoop/Desktop/Customer_support_tickets.csv
```

Output:

```text
/data/bronze/customer_support_tickets
```

### Offers

```text
GetFile
   -> LogAttribute
   -> PutHDFS
```

Input:

```text
/home/hadoop/Desktop/Offers.csv
```

Output:

```text
/data/bronze/offers
```

### Usage

```text
GetFile
   -> LogAttribute
   -> PutHDFS
```

Input:

```text
/home/hadoop/Desktop/Usage.json
```

Output:

```text
/data/bronze/usage
```

## HDFS Configuration

The `PutHDFS` processors use the Hadoop configuration files:

```text
/home/hadoop/hadoop/etc/hadoop/core-site.xml
/home/hadoop/hadoop/etc/hadoop/hdfs-site.xml
```

The HDFS Bronze root directory is:

```text
/data/bronze
```

The expected directory structure is:

```text
/data/bronze/
├── customer_support_tickets/
├── offers/
└── usage/
```

## Processor Scheduling

The current template uses **Timer Driven** scheduling.

The processors are configured with a schedule value in seconds. A processor that is scheduled every `10000` seconds will run approximately every 2 hours and 46 minutes.

For regular development and testing, a shorter polling interval such as `10` seconds is more practical. The schedule should be adjusted according to the required deployment behavior.

## GetFile Configuration

The `GetFile` processors are configured with:

- Input directory: `/home/hadoop/Desktop`
- Recursive search: enabled
- Keep Source File: enabled
- Batch Size: `10`
- Filename filtering for the required source file

Keeping the source file means NiFi does not delete the original file after it is successfully picked up.

## PutHDFS Configuration

The `PutHDFS` processors write the incoming FlowFiles to their corresponding HDFS Bronze directories.

The conflict resolution strategy is configured as:

```text
replace
```

This means an existing file with the same name can be replaced during ingestion.

## Prerequisites

Before using the template, make sure that:

1. Hadoop HDFS is running.
2. The Bronze directories exist.
3. NiFi can access the source directory.
4. The NiFi process has permission to write to HDFS.
5. The three source files exist in `/home/hadoop/Desktop`.

Check HDFS with:

```bash
jps
hdfs dfs -ls /data/bronze
```

Check the source files with:

```bash
ls -lh /home/hadoop/Desktop/Customer_support_tickets.csv
ls -lh /home/hadoop/Desktop/Offers.csv
ls -lh /home/hadoop/Desktop/Usage.json
```

## Running the Template

Import the NiFi template into the NiFi UI, place the processors on the canvas, and start the required processors.

The expected flow is:

```text
Desktop Source Files
        |
        v
     GetFile
        |
        v
  LogAttribute
        |
        v
     PutHDFS
        |
        v
   HDFS Bronze Layer
```

After running the flow, verify the Bronze directories:

```bash
hdfs dfs -ls /data/bronze/customer_support_tickets
hdfs dfs -ls /data/bronze/offers
hdfs dfs -ls /data/bronze/usage
```

## Expected Result

A successful ingestion places the three source datasets into HDFS Bronze:

```text
/data/bronze/customer_support_tickets/
/data/bronze/offers/
/data/bronze/usage/
```

These Bronze datasets are then used by the Spark Bronze-to-Silver transformation stage.

## Troubleshooting

### Permission denied when writing to HDFS

Check ownership and permissions:

```bash
hdfs dfs -ls -d /data/bronze
```

The NiFi user must have permission to write to the Bronze directories.

For the current environment, NiFi may run as `root`. The Bronze directory can be configured accordingly, for example:

```bash
hdfs dfs -chown root:supergroup /data/bronze
hdfs dfs -chmod 775 /data/bronze
```

### No FlowFiles are created

Check that the source files exist:

```bash
ls -lh /home/hadoop/Desktop
```

Then verify the filename filters in the `GetFile` processors.

### PutHDFS fails

Check that HDFS is running:

```bash
jps
```

Check the Hadoop NameNode and DataNode processes and verify that the configured `core-site.xml` and `hdfs-site.xml` paths are correct.

## Important Notes

- This template is for **NiFi ingestion only**.
- `Customers.csv` is ingested through **MariaDB + Sqoop**, not NiFi.
- NiFi writes the raw datasets to the **Bronze layer** without performing the Silver or Gold transformations.
- The template is a snapshot of the NiFi flow. Changes made to a running NiFi flow are not automatically synchronized back to the exported template file. Export the template again after important changes.

## Project Pipeline Position

```text
Customer.csv
   |
MariaDB
   |
Sqoop
   |
HDFS Bronze

Tickets.csv ----\
Offers.csv ------- > NiFi ----> HDFS Bronze
Usage.json ------/

HDFS Bronze
     |
     v
Spark Bronze -> Silver
     |
     v
Spark Silver -> Gold
     |
     v
Hive / Analytics
```

## Template File

The NiFi template should be stored in the repository as:

```text
nifi/nifi-ingestion.xml
```

## Author

Churn Customer ETL Project
