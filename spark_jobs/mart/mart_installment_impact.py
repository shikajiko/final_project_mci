from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark       = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data = client_dwh.query("SELECT order_id, payment_installments, payment_value, is_installment, installment_bucket FROM fact_payments")
payments = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

ord_data = client_dwh.query("SELECT order_id, total_order_value FROM fact_orders")
orders   = spark.createDataFrame(ord_data.result_rows, schema=ord_data.column_names)
orders   = orders.filter(F.col("total_order_value").isNotNull())
orders   = orders.withColumnRenamed("total_order_value", "order_total_value")

joined = payments.join(orders, on="order_id", how="left")

total_rev = joined.agg(F.sum("payment_value")).collect()[0][0]

df = (
    joined
    .groupBy("installment_bucket")
    .agg(
        F.count("order_id").alias("total_transactions"),
        F.round(F.avg("payment_value"), 2).alias("avg_payment_value"),
        F.round(F.avg("order_total_value"), 2).alias("avg_order_value"),
        F.round(F.avg("payment_installments"), 2).alias("avg_installments"),
        F.round(F.sum("payment_value"), 2).alias("total_revenue"),
    )
    .withColumn(
        "pct_of_total_revenue",
        F.round(F.col("total_revenue") / F.lit(total_rev) * 100, 2)
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.installment_impact")
client_mart.insert_df("installment_impact", df.toPandas())

spark.stop()