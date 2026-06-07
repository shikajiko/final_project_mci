import clickhouse_connect
import os

def get_client(database: str = "default"):
    return clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        database = database
    )