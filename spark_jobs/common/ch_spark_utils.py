from pyspark.sql import SparkSession
import clickhouse_connect
import os

CLICKHOUSE_PROPERTIES = {
    "user": os.getenv("CLICKHOUSE_USER"),
    "password": os.getenv("CLICKHOUSE_PASSWORD")
}


def get_spark():
    return (
        SparkSession.builder
        .appName("groceria")
        .getOrCreate()
    )

def get_client(database: str = "default"):
    return clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username=CLICKHOUSE_PROPERTIES["user"],
        password=CLICKHOUSE_PROPERTIES["password"],
        database=database
    )