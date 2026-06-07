import sys

from pyspark.sql import SparkSession

from common.ch_spark_utils import (
    CLICKHOUSE_URL,
    CLICKHOUSE_PROPERTIES
)

csv_path = sys.argv[1]
target_table = sys.argv[2]

spark = (
    SparkSession.builder
    .appName("raw-ingestion")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(csv_path)
)

(
    df.write
    .mode("append")
    .jdbc(
        url=CLICKHOUSE_URL,
        table=target_table,
        properties=CLICKHOUSE_PROPERTIES
    )
)

spark.stop()