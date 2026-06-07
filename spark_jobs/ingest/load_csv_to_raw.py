import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.ch_spark_utils import CLICKHOUSE_PROPERTIES, get_spark, get_client

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

client = get_client(db)
client.insert_df(table_name, df.toPandas())

spark.stop()