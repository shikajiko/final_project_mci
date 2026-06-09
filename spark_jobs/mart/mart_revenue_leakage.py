from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark       = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

ord_data  = client_dwh.query("SELECT order_id, order_status FROM fact_orders")
orders    = spark.createDataFrame(ord_data.result_rows, schema=ord_data.column_names)

pay_data  = client_dwh.query("SELECT order_id, payment_type, payment_value, is_installment FROM fact_payments")
payments  = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

df = (
    payments
    .join(orders, on="order_id", how="left")
    .groupBy("order_status", "payment_type")
    .agg(
        F.count("order_id").alias("transaction_count"),
        F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
        F.round(F.mean("payment_value"), 2).alias("avg_payment_value"),
        F.sum("is_installment").alias("installment_count"),
        F.round(F.sum("is_installment") * 100.0 / F.count("order_id"), 2).alias("pct_installment"),
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.revenue_leakage")
client_mart.insert_df("revenue_leakage", df.toPandas())

spark.stop()