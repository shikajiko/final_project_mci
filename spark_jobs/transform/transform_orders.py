import sys
import os
from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_raw = get_client("raw")
client_ods = get_client("ods")

data = client_raw.query("SELECT * FROM orders")
df = spark.createDataFrame(data.result_rows, schema=data.column_names)

timestamp_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

for col in timestamp_cols:
    df = df.withColumn(col, F.to_timestamp(F.col(col)))

df = (
    df
    .withColumn(
        "days_to_deliver_actual",
        (F.unix_timestamp("order_delivered_customer_date") -
         F.unix_timestamp("order_purchase_timestamp")) / 86400.0
    )
    .withColumn(
        "days_to_deliver_estimated",
        (F.unix_timestamp("order_estimated_delivery_date") -
         F.unix_timestamp("order_purchase_timestamp")) / 86400.0
    )
    .withColumn(
        "delivery_delay_days",
        (F.unix_timestamp("order_delivered_customer_date") -
         F.unix_timestamp("order_estimated_delivery_date")) / 86400.0
    )
    .withColumn(
        "is_delivered",
        F.when(F.col("order_status") == "delivered", 1).otherwise(0)
    )
    .withColumn(
        "is_late",
        F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
    )
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
)

client_ods.command("TRUNCATE TABLE IF EXISTS ods.orders")
client_ods.insert_df("orders", df.toPandas())
spark.stop()