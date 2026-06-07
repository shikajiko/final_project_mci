from pyspark.sql import SparkSession

CLICKHOUSE_PROPERTIES = {
    "user": "admin",
    "password": "rahasia"
}

def get_spark():
    return (
        SparkSession.builder
        .appName("groceria")
        .getOrCreate()
    )