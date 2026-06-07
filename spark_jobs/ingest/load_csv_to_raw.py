import sys
import os
import clickhouse_connect

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.ch_spark_utils import CLICKHOUSE_PROPERTIES, get_spark

csv_path = sys.argv[1]
target_table = sys.argv[2]

if "." in target_table:
    db, table_name = target_table.split(".", 1)
else:
    db = "default"
    table_name = target_table

spark = get_spark()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("multiLine", True)        
    .option("quote", '"')             
    .option("escape", '"')            
    .csv(csv_path)
)

client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username=CLICKHOUSE_PROPERTIES["user"],
    password=CLICKHOUSE_PROPERTIES["password"],
    database=db
)

client.insert_df(table_name, df.toPandas())

spark.stop()