from clickhouse_driver import Client

def get_client():
    return Client(
        host="clickhouse",
        port=9000,
        user="admin",
        password="rahasia",
        database="default"
    )