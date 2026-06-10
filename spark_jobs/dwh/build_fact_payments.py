from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_ods = get_client("ods")
client_dwh = get_client("dwh")

pay_data = client_ods.query("SELECT * FROM order_payments")
payments = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

orders_data = client_ods.query("SELECT order_id, customer_id, order_purchase_timestamp FROM orders")
orders = spark.createDataFrame(orders_data.result_rows, schema=orders_data.column_names)

cust_data = client_ods.query("SELECT customer_id, customer_state, lat, lng FROM customers")
customers = spark.createDataFrame(cust_data.result_rows, schema=cust_data.column_names)

items_data = client_ods.query("SELECT order_id, price, freight_value FROM order_items")
items = spark.createDataFrame(items_data.result_rows, schema=items_data.column_names)

items_agg = (
    items
    .groupBy("order_id")
    .agg(
        (F.sum("price") + F.sum("freight_value")).alias("total_order_value")
    )
)

fact_payments = (
    payments
    .join(orders,    on="order_id", how="left")
    .join(customers, on="customer_id", how="left")
    .join(items_agg, on="order_id", how="left")
    .withColumn(
        "date_id",
        F.date_format(F.to_timestamp("order_purchase_timestamp"), "yyyy-MM-dd")
    )
    .select(
        "order_id",
        "customer_id",
        "date_id",
        "payment_type",
        "payment_sequential",
        "payment_installments",
        "payment_value",
        "is_installment",
        "installment_bucket",
        "is_high_value_payment",
        "total_order_value",
        "customer_state",
        "lat",
        "lng",
    )
)

client_dwh.command("TRUNCATE TABLE IF EXISTS dwh.fact_payments")
client_dwh.insert_df("fact_payments", fact_payments.toPandas())

spark.stop()