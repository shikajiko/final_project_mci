from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_raw = get_client("raw")
client_ods = get_client("ods")

data = client_raw.query("SELECT * FROM order_items")
df = spark.createDataFrame(data.result_rows, schema=data.column_names)

df = (
    df
    .withColumn("order_item_id",      F.col("order_item_id").cast("int"))
    .withColumn("price",              F.col("price").cast("double"))
    .withColumn("freight_value",      F.col("freight_value").cast("double"))
    .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
    .withColumn("line_total",         F.col("price") + F.col("freight_value"))
    .withColumn(
        "freight_ratio",
        F.when(F.col("price") > 0, F.col("freight_value") / F.col("price"))
         .otherwise(F.lit(None).cast("double"))
    )
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
)

client_ods.command("TRUNCATE TABLE IF EXISTS ods.order_items")
client_ods.insert_df("order_items", df.toPandas())
spark.stop()