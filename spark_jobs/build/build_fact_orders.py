from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_ods = get_client("ods")
client_dwh = get_client("dwh")

orders_data = client_ods.query("SELECT * FROM orders")
orders = spark.createDataFrame(orders_data.result_rows, schema=orders_data.column_names)

items_data = client_ods.query("SELECT * FROM order_items")
items = spark.createDataFrame(items_data.result_rows, schema=items_data.column_names)

items_agg = (
    items
    .groupBy("order_id")
    .agg(
        F.sum("price").alias("total_order_value"),
        F.sum("freight_value").alias("total_freight"),
        F.count("order_item_id").alias("item_count"),
    )
    .withColumn("total_order_revenue", F.col("total_order_value") + F.col("total_freight"))
)

fact_orders = (
    orders
    .join(items_agg, on="order_id", how="left")
    .withColumn(
        "date_id",
        F.date_format(F.to_timestamp("order_purchase_timestamp"), "yyyy-MM-dd")
    )
    .select(
        "order_id",
        "customer_id",
        "date_id",
        "order_status",
        "total_order_value",
        "total_freight",
        "total_order_revenue",
        "item_count",
        "days_to_deliver_actual",
        "days_to_deliver_estimated",
        "delivery_delay_days",
        "is_delivered",
        "is_late",
    )
)

client_dwh.command("TRUNCATE TABLE IF EXISTS dwh.fact_orders")
client_dwh.insert_df("fact_orders", fact_orders.toPandas())

spark.stop()