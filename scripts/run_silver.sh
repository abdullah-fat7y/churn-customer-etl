#!/bin/bash
set -e
export SAFE_MODE=0
export DEBUG=1
/usr/local/spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
/home/hadoop/Desktop/bronze_to_silver.py
